//go:build client
// +build client

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Client struct {
	config         *ClientConfig
	logger         *Logger
	reconnectCount uint64
}

func main() {
	configPath := flag.String("config", "client.json", "Path to config file")
	flag.Parse()

	logger := NewLogger("CLIENT")

	config, err := LoadClientConfig(*configPath)
	if err != nil {
		logger.Fatal("Failed to load config: %v", err)
	}

	logger.Info("Config loaded and validated successfully")

	client := &Client{
		config:         config,
		logger:         logger,
		reconnectCount: 0,
	}

	logger.Info("Bleep Proxy Client v%s", Version)
	logger.Info("Server: %s", config.Server)
	logger.Info("Local: %s", config.Local)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logger.Info("Shutting down...")
		cancel()
	}()

	client.Run(ctx)
	logger.Info("Shutdown complete")
}

func (c *Client) Run(ctx context.Context) {
	backoff := RetryInterval
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.sessionLoop(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}

			atomic.AddUint64(&c.reconnectCount, 1)
			c.logger.Error("Session error: %v. Reconnect #%d. Retrying in %v...",
				err, atomic.LoadUint64(&c.reconnectCount), backoff)

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
			backoff = RetryInterval
			atomic.StoreUint64(&c.reconnectCount, 0)
			select {
			case <-ctx.Done():
				return
			case <-time.After(RetryInterval):
			}
		}
	}
}

func (c *Client) sessionLoop(ctx context.Context) error {
	mux, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer mux.Close()

	exitChan := make(chan struct{})
	go func() {
		mux.ReadLoop(func(frame *Frame) {
			if frame.Type == FrameTypeOpen {
				priority := 0
				if len(frame.Data) > 1 {
					priority = int(frame.Data[1])
				}
				stream := mux.RegisterChannel(frame.ChannelID, priority)
				go c.handleOpen(ctx, mux, frame, stream)
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

func (c *Client) connect(ctx context.Context) (*Multiplexer, error) {
	c.logger.Info("Connecting to %s...", c.config.Server)

	dialer := net.Dialer{Timeout: DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.config.Server)
	if err != nil {
		return nil, err
	}

	if err := ConfigureTCPConn(conn); err != nil {
		c.logger.Warn("Failed to configure TCP: %v", err)
	}

	if err := c.authenticate(conn); err != nil {
		conn.Close()
		return nil, err
	}

	c.logger.Info("Authenticated successfully")
	return NewMultiplexer(conn), nil
}

func (c *Client) authenticate(conn net.Conn) error {
	payload := fmt.Sprintf("%s:%s\n", c.config.ID, c.config.Key)
	if _, err := conn.Write([]byte(payload)); err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(DialTimeout))
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

func (c *Client) handleOpen(ctx context.Context, mux *Multiplexer, frame *Frame, stream chan *Frame) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic in handleOpen: %v", r)
		}
	}()

	proto := ProtocolTCP
	priority := 0
	if len(frame.Data) > 0 {
		proto = Protocol(frame.Data[0])
	}
	if len(frame.Data) > 1 {
		priority = int(frame.Data[1])
	}

	c.logger.Info("OPEN processing: Proto=%s, CH=%d, Priority=%d", proto.String(), frame.ChannelID, priority)

	dialCtx, cancel := context.WithTimeout(ctx, LocalDialTimeout)
	defer cancel()

	c.logger.Info("Dialing %s...", c.config.Local)
	conn, err := c.dial(dialCtx, proto)
	if err != nil {
		c.logger.Error("Failed to connect to local %s: %v", c.config.Local, err)
		mux.WriteFrame(&Frame{Type: FrameTypeClose, ChannelID: frame.ChannelID})
		mux.CloseChannel(frame.ChannelID)
		return
	}
	defer conn.Close()

	c.logger.Info("Connected to local %s for CH %d", c.config.Local, frame.ChannelID)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.pipeUp(mux, conn, frame.ChannelID)
		mux.CloseChannel(frame.ChannelID)
		c.logger.Debug("Local->Tunnel pipe closed for CH %d", frame.ChannelID)
	}()

	go func() {
		defer wg.Done()
		c.pipeDown(conn, stream)
		c.logger.Debug("Tunnel->Local pipe closed for CH %d", frame.ChannelID)
	}()

	wg.Wait()
	c.logger.Info("Local tunnel closed: CH %d", frame.ChannelID)
}

func (c *Client) dial(ctx context.Context, proto Protocol) (net.Conn, error) {
	if proto == ProtocolUDP {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		addr, err := net.ResolveUDPAddr("udp", c.config.Local)
		if err != nil {
			pc.Close()
			return nil, err
		}
		return &UDPConnWrapper{
			PacketConn: pc,
			target:     addr,
		}, nil
	}

	dialer := net.Dialer{Timeout: LocalDialTimeout}
	return dialer.DialContext(ctx, proto.String(), c.config.Local)
}

func (c *Client) pipeUp(mux *Multiplexer, conn net.Conn, chID uint32) {
	buf := make([]byte, ReadBufferSize)
	totalBytes := uint64(0)
	packetCount := 0
	
	for {
		n, err := conn.Read(buf)
		
		if n > 0 {
			packetCount++
			totalBytes += uint64(n)
			
			data := make([]byte, n)
			copy(data, buf[:n])
			if err := mux.WriteFrame(&Frame{
				Type:      FrameTypeData,
				ChannelID: chID,
				Data:      data,
			}); err != nil {
				c.logger.Error("CH %d: Write failed: %v", chID, err)
				return
			}
		}
		
		if err != nil {
			c.logger.Info("CH %d: Closed after %d packets, %d bytes", chID, packetCount, totalBytes)
			mux.WriteFrame(&Frame{Type: FrameTypeClose, ChannelID: chID})
			return
		}
	}
}

func (c *Client) pipeDown(conn net.Conn, stream chan *Frame) {
	totalBytes := uint64(0)
	frameCount := 0
	var chID uint32
	
	for frame := range stream {
		chID = frame.ChannelID
		frameCount++
		
		if frame.Type != FrameTypeData {
			return
		}
		
		totalBytes += uint64(len(frame.Data))
		if _, err := conn.Write(frame.Data); err != nil {
			c.logger.Error("CH %d: Write failed: %v", chID, err)
			return
		}
	}
	c.logger.Info("CH %d: Closed after %d frames, %d bytes", chID, frameCount, totalBytes)
}
