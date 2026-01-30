//go:build client
// +build client

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	defaultConfigPath  = "client.json"
	readBufferSize     = 65535
	dialTimeout        = 10 * time.Second
	localDialTimeout   = 5 * time.Second
	keepAlivePeriod    = 30 * time.Second
	retryInterval      = 5 * time.Second
	shutdownTimeout    = 3 * time.Second
	connectionTimeout  = 60 * time.Second 
)

type Config struct {
	AuthKey        string `json:"auth_key"`
	ClientID       string `json:"client_id"`
	ServiceAddress string `json:"service_address"`
	ServerAddress  string `json:"server_address"`
	ServerPort     string `json:"server_port"`
}

type UpstreamConnector interface {
	Connect(ctx context.Context) (*Multiplexer, error)
}

type BleepConnector struct {
	config *Config
}

func (c *BleepConnector) Connect(ctx context.Context) (*Multiplexer, error) {
	addr := fmt.Sprintf("%s:%s", c.config.ServerAddress, c.config.ServerPort)
	dialer := net.Dialer{Timeout: dialTimeout}
	
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(keepAlivePeriod)
	}

	if err := c.authenticate(conn); err != nil {
		conn.Close()
		return nil, err
	}

	return NewMultiplexer(conn), nil
}

func (c *BleepConnector) authenticate(conn net.Conn) error {
	payload := fmt.Sprintf("%s:%s\n", c.config.ClientID, c.config.AuthKey)
	if _, err := conn.Write([]byte(payload)); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(dialTimeout))
	defer conn.SetReadDeadline(time.Time{})

	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		return fmt.Errorf("failed to read auth response: %v", err)
	}

	if buf[0] == 0 {
		return fmt.Errorf("auth rejected by server")
	}
	
	if buf[0] != 1 {
		return fmt.Errorf("unexpected auth response: %v", buf[0])
	}

	return nil
}

type DownstreamDialer interface {
	Dial(ctx context.Context, proto Protocol) (net.Conn, error)
}

type LocalDialer struct {
	target string
}

func (d *LocalDialer) Dial(ctx context.Context, proto Protocol) (net.Conn, error) {
    if proto == ProtocolUDP {
        pc, err := net.ListenPacket("udp", "127.0.0.1:0")
        if err != nil {
            return nil, err
        }
        
        addr, err := net.ResolveUDPAddr("udp", d.target)
        if err != nil {
            pc.Close()
            return nil, err
        }

        return &udpConnWrapper{
            PacketConn: pc,
            target:     addr,
        }, nil
    }
    
    dialer := net.Dialer{Timeout: localDialTimeout}
    return dialer.DialContext(ctx, proto.String(), d.target)
}

type Agent struct {
	config    *Config
	connector UpstreamConnector
	dialer    DownstreamDialer
}

func NewAgent(cfg *Config) *Agent {
	return &Agent{
		config:    cfg,
		connector: &BleepConnector{config: cfg},
		dialer:    &LocalDialer{target: cfg.ServiceAddress},
	}
}

func (a *Agent) Run(ctx context.Context) {
	log.Printf("Starting Agent for %s -> %s", a.config.ClientID, a.config.ServiceAddress)

	backoff := retryInterval
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := a.sessionLoop(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[Agent] Session error: %v. Retrying in %v...", err, backoff)

				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}

				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			} else {
				backoff = retryInterval
				select {
				case <-ctx.Done():
					return
				case <-time.After(retryInterval):
				}
			}
		}
	}
}

func (a *Agent) sessionLoop(ctx context.Context) error {
    mux, err := a.connector.Connect(ctx)
    if err != nil {
        return err
    }
    defer mux.Close()

    exitChan := make(chan struct{})
    go func() {
        mux.ReadLoop(func(frame *Frame) {
            if frame.Type == FrameTypeOpen {
                go a.handleOpen(ctx, mux, frame)
            }
        })
        close(exitChan)
    }()

    select {
    case <-ctx.Done():
        mux.Close() 
        return ctx.Err()
    case <-exitChan:
        return fmt.Errorf("multiplexer read loop stopped")
    }
}

func (a *Agent) handleOpen(ctx context.Context, mux *Multiplexer, frame *Frame) {
    proto := ProtocolTCP
    priority := 0

    if len(frame.Data) > 0 {
        proto = Protocol(frame.Data[0])
    }
    if len(frame.Data) > 1 {
        priority = int(frame.Data[1])
    }

    frame.Priority = priority

    log.Printf("Opening local tunnel: Proto=%s, ID=%d", proto.String(), frame.ChannelID)

    dialCtx, cancel := context.WithTimeout(ctx, localDialTimeout)
    defer cancel()

    conn, err := a.dialer.Dial(dialCtx, proto)
    if err != nil {
        log.Printf("Could not connect to local service at %s: %v", a.config.ServiceAddress, err)
        mux.WriteFrame(&Frame{Type: FrameTypeClose, ChannelID: frame.ChannelID})
        return
    }

    stream := mux.RegisterChannel(frame.ChannelID, frame.Priority)

    var wg sync.WaitGroup
    wg.Add(2)

    go func() {
        defer wg.Done()
        a.pipeUp(mux, conn, frame.ChannelID)
        mux.CloseChannel(frame.ChannelID) 
    }()

    go func() {
        defer wg.Done()
        a.pipeDown(conn, stream)
    }()

    go func() {
        wg.Wait()
        conn.Close()
        log.Printf("Local tunnel closed: ID=%d", frame.ChannelID)
    }()
}

func (a *Agent) pipeUp(mux *Multiplexer, conn net.Conn, chID uint32) {
    buf := make([]byte, readBufferSize)
    for {
        n, err := conn.Read(buf)
        if n > 0 {
            data := make([]byte, n)
            copy(data, buf[:n])
            if err := mux.WriteFrame(&Frame{Type: FrameTypeData, ChannelID: chID, Data: data}); err != nil {
                log.Printf("[CH %d] Tunnel write error (connection lost), stopping pipeUp", chID)
                return 
            }
        }
        if err != nil {
            mux.WriteFrame(&Frame{Type: FrameTypeClose, ChannelID: chID})
            return
        }
    }
}

func (a *Agent) pipeDown(conn net.Conn, stream chan *Frame) {
	for frame := range stream {
		if frame.Type != FrameTypeData {
			return
		}

		if _, err := conn.Write(frame.Data); err != nil {
			log.Printf("[CH %d] Local write error: %v", frame.ChannelID, err)
			return
		}
	}
}

func main() {
	configPath := flag.String("config", defaultConfigPath, "Path to config file")
	flag.Parse()

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("Read config: %v", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Parse config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	agent := NewAgent(&cfg)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		cancel()
	}()

	agent.Run(ctx)
}

type udpConnWrapper struct {
    net.PacketConn
    target net.Addr
}

func (w *udpConnWrapper) Read(b []byte) (int, error) {
    n, _, err := w.ReadFrom(b)
    return n, err
}

func (w *udpConnWrapper) Write(b []byte) (int, error) {
    return w.WriteTo(b, w.target)
}

func (w *udpConnWrapper) LocalAddr() net.Addr  { return w.PacketConn.LocalAddr() }
func (w *udpConnWrapper) RemoteAddr() net.Addr { return w.target }
func (w *udpConnWrapper) SetDeadline(t time.Time) error      { return w.PacketConn.SetDeadline(t) }
func (w *udpConnWrapper) SetReadDeadline(t time.Time) error  { return w.PacketConn.SetReadDeadline(t) }
func (w *udpConnWrapper) SetWriteDeadline(t time.Time) error { return w.PacketConn.SetWriteDeadline(t) }