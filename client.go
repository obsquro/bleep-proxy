//go:build client
// +build client

package main

import (
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

type ClientConfig struct {
	AuthKey        string `json:"auth_key"`
	ClientID       string `json:"client_id"`
	ServiceAddress string `json:"service_address"`
	ServerAddress  string `json:"server_address"`
	ServerPort     string `json:"server_port"`
}

type Client struct {
	config      ClientConfig
	mux         *Multiplexer
	mu          sync.Mutex
	stop        chan struct{}
	activeConns sync.WaitGroup
}

func main() {
	configPath := flag.String("config", "client.json", "Path to config file")
	flag.Parse()

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("Failed to read config: %v", err)
	}

	var config ClientConfig
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	client := &Client{
		config: config,
		stop:   make(chan struct{}),
	}

	log.Printf("Starting Bleep Proxy Client")
	log.Printf("Server: %s:%s", config.ServerAddress, config.ServerPort)
	log.Printf("Local Service: %s", config.ServiceAddress)

	go client.run()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	close(client.stop)

	client.mu.Lock()
	if client.mux != nil {
		client.mux.Close()
	}
	client.mu.Unlock()

	done := make(chan struct{})
	go func() {
		client.activeConns.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Shutdown complete")
	case <-time.After(3 * time.Second):
		log.Println("Shutdown timeout - forcing exit")
	}
}

func (c *Client) run() {
	for {
		select {
		case <-c.stop:
			return
		default:
		}

		if err := c.connect(); err != nil {
			select {
			case <-c.stop:

				return
			default:
				log.Printf("Connection error: %v", err)
			}
		}

		select {
		case <-c.stop:
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (c *Client) connect() error {
	serverAddr := fmt.Sprintf("%s:%s", c.config.ServerAddress, c.config.ServerPort)
	conn, err := net.DialTimeout("tcp", serverAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("dial server: %w", err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	authData := fmt.Sprintf("%s:%s\n", c.config.ClientID, c.config.AuthKey)
	if _, err := conn.Write([]byte(authData)); err != nil {
		conn.Close()
		return fmt.Errorf("send auth: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response := make([]byte, 1)
	n, err := conn.Read(response)
	if err == nil && n > 0 && response[0] == 0 {
		conn.Close()
		log.Fatalf("Authentication failed: invalid auth_key or client_id")
	}
	conn.SetReadDeadline(time.Time{})

	log.Printf("Connected to server")

	c.mu.Lock()
	c.mux = NewMultiplexer(conn)
	c.mu.Unlock()

	c.mux.ReadLoop(c.handleFrame)

	return fmt.Errorf("connection closed")
}

func (c *Client) handleFrame(frame *Frame) {
	switch frame.Type {
	case FrameTypeOpen:
		c.activeConns.Add(1)
		go c.handleNewChannel(frame.ChannelID)

	case FrameTypeData:
		c.mux.SafeSend(frame.ChannelID, frame)

	case FrameTypeClose:
		c.mux.CloseChannel(frame.ChannelID)
	}
}

func (c *Client) handleNewChannel(channelID uint32) {
	defer c.activeConns.Done()

	localConn, err := net.DialTimeout("tcp", c.config.ServiceAddress, 5*time.Second)
	if err != nil {
		c.mux.WriteFrame(&Frame{
			Type:      FrameTypeClose,
			ChannelID: channelID,
		})
		return
	}
	defer localConn.Close()

	if tcpConn, ok := localConn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	dataChan := c.mux.RegisterChannel(channelID)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, 32768)
		for {
			n, err := localConn.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				if err := c.mux.WriteFrame(&Frame{
					Type:      FrameTypeData,
					ChannelID: channelID,
					Data:      data,
				}); err != nil {
					return
				}
			}
			if err != nil {
				break
			}
		}
		c.mux.WriteFrame(&Frame{
			Type:      FrameTypeClose,
			ChannelID: channelID,
		})
	}()

	go func() {
		defer wg.Done()
		for frame := range dataChan {
			if frame.Type == FrameTypeData && len(frame.Data) > 0 {
				localConn.Write(frame.Data)
			} else if frame.Type == FrameTypeClose {
				localConn.Close()
				break
			}
		}
	}()

	wg.Wait()
	c.mux.CloseChannel(channelID)
}
