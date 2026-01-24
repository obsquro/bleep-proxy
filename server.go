//go:build server
// +build server

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type PortConfig struct {
	Port     int    `json:"port"`
	Key      string `json:"key"`
	Protocol string `json:"protocol"`
}

type ServerConfig struct {
	ClientListenPort  string                `json:"client_listen_port"`
	PublicListenPorts map[string]PortConfig `json:"public_listen_ports"`
}

type Server struct {
	config         ServerConfig
	clients        map[string]*Multiplexer
	clientsMutex   sync.RWMutex
	wg             sync.WaitGroup
	stop           chan struct{}
	listeners      []net.Listener
	listenersMutex sync.Mutex
}

func main() {
	configPath := flag.String("config", "server.json", "Path to config file")
	flag.Parse()

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("Failed to read config: %v", err)
	}

	var config ServerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	server := &Server{
		config:  config,
		clients: make(map[string]*Multiplexer),
		stop:    make(chan struct{}),
	}

	log.Printf("Starting Bleep Proxy Server")
	log.Printf("Client port: %s", config.ClientListenPort)
	log.Printf("Public ports: %d", len(config.PublicListenPorts))

	server.wg.Add(1)
	go server.listenForClients()

	for clientID, portCfg := range config.PublicListenPorts {
		server.wg.Add(1)
		go server.listenForPublic(clientID, portCfg)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	close(server.stop)
	server.closeAllListeners()
	server.closeAllClients()
	server.wg.Wait()
	log.Println("Shutdown complete")
}

func (s *Server) closeAllListeners() {
	s.listenersMutex.Lock()
	defer s.listenersMutex.Unlock()

	for _, listener := range s.listeners {
		listener.Close()
	}
	log.Printf("Closed %d listeners", len(s.listeners))
}

func (s *Server) closeAllClients() {
	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()

	for clientID, mux := range s.clients {
		mux.Close()
		log.Printf("Closed client %s", clientID)
	}
}

func (s *Server) listenForClients() {
	defer s.wg.Done()

	listener, err := net.Listen("tcp", ":"+s.config.ClientListenPort)
	if err != nil {
		log.Fatalf("Failed to listen for clients: %v", err)
	}
	defer listener.Close()

	s.listenersMutex.Lock()
	s.listeners = append(s.listeners, listener)
	s.listenersMutex.Unlock()

	log.Printf("Listening for clients on :%s", s.config.ClientListenPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				log.Printf("Accept client error: %v", err)
				continue
			}
		}

		go s.handleClient(conn)
	}
}

func (s *Server) handleClient(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)
	authLine, err := reader.ReadString('\n')
	conn.SetReadDeadline(time.Time{})

	if err != nil {
		log.Printf("Failed to read auth: %v", err)
		conn.Close()
		return
	}

	authLine = strings.TrimSpace(authLine)
	parts := strings.SplitN(authLine, ":", 2)
	if len(parts) != 2 {
		log.Printf("Invalid auth format")
		conn.Close()
		return
	}

	clientID := parts[0]
	authKey := parts[1]

	s.clientsMutex.RLock()
	portCfg, exists := s.config.PublicListenPorts[clientID]
	s.clientsMutex.RUnlock()

	if !exists || portCfg.Key != authKey {
		log.Printf("Auth failed for client %s", clientID)
		conn.Write([]byte{0})
		conn.Close()
		return
	}

	log.Printf("Client %s authenticated", clientID)

	mux := NewMultiplexer(conn)

	s.clientsMutex.Lock()
	if oldMux, exists := s.clients[clientID]; exists {
		oldMux.Close()
	}
	s.clients[clientID] = mux
	s.clientsMutex.Unlock()

	mux.ReadLoop(func(frame *Frame) {
		s.handleClientFrame(clientID, frame)
	})

	s.clientsMutex.Lock()
	delete(s.clients, clientID)
	s.clientsMutex.Unlock()

	log.Printf("Client %s disconnected", clientID)
}

func (s *Server) handleClientFrame(clientID string, frame *Frame) {

	s.clientsMutex.RLock()
	mux := s.clients[clientID]
	s.clientsMutex.RUnlock()

	if mux == nil {
		return
	}

	mux.SafeSend(frame.ChannelID, frame)
}

func (s *Server) listenForPublic(clientID string, portCfg PortConfig) {
	if portCfg.Protocol == "udp" {
		s.listenForPublicUDP(clientID, portCfg)
		return
	}

	defer s.wg.Done()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", portCfg.Port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", portCfg.Port, err)
	}
	defer listener.Close()

	s.listenersMutex.Lock()
	s.listeners = append(s.listeners, listener)
	s.listenersMutex.Unlock()

	log.Printf("Listening for public on :%d (client: %s)", portCfg.Port, clientID)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				log.Printf("Accept public error: %v", err)
				continue
			}
		}

		go s.handlePublic(conn, clientID)
	}
}

func (s *Server) handlePublic(publicConn net.Conn, clientID string) {
	defer publicConn.Close()

	s.clientsMutex.RLock()
	mux, exists := s.clients[clientID]
	s.clientsMutex.RUnlock()

	if !exists || mux == nil {
		log.Printf("No client connected for %s", clientID)
		return
	}

	channelID, dataChan := mux.OpenChannel()

	if err := mux.WriteFrame(&Frame{
		Type:      FrameTypeOpen,
		ChannelID: channelID,
		Data:      []byte{ProtocolTCP},
	}); err != nil {
		mux.CloseChannel(channelID)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, 32768)
		for {
			n, err := publicConn.Read(buf)
			if n > 0 {
				if err := mux.WriteFrame(&Frame{
					Type:      FrameTypeData,
					ChannelID: channelID,
					Data:      buf[:n],
				}); err != nil {
					return
				}
			}
			if err != nil {
				break
			}
		}
		mux.WriteFrame(&Frame{
			Type:      FrameTypeClose,
			ChannelID: channelID,
		})
	}()

	go func() {
		defer wg.Done()
		for frame := range dataChan {
			if frame.Type == FrameTypeData && len(frame.Data) > 0 {
				publicConn.Write(frame.Data)
			} else if frame.Type == FrameTypeClose {
				break
			}
		}
	}()

	wg.Wait()
	mux.CloseChannel(channelID)
}

func (s *Server) listenForPublicUDP(clientID string, portCfg PortConfig) {
	defer s.wg.Done()

	addr := fmt.Sprintf(":%d", portCfg.Port)
	pconn, err := net.ListenPacket("udp", addr)
	if err != nil {
		log.Fatalf("Failed to listen UDP on %s: %v", addr, err)
	}

	log.Printf("Listening for public UDP on %s (client: %s)", addr, clientID)

	go func() {
		<-s.stop
		pconn.Close()
	}()

	type udpSession struct {
		channelID  uint32
		lastActive time.Time
	}

	sessions := make(map[string]*udpSession)
	var sessMu sync.Mutex

	// Cleanup routine for inactive sessions
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				sessMu.Lock()
				now := time.Now()
				for key, sess := range sessions {
					if now.Sub(sess.lastActive) > 60*time.Second {
						s.clientsMutex.RLock()
						mux := s.clients[clientID]
						s.clientsMutex.RUnlock()
						if mux != nil {
							mux.CloseChannel(sess.channelID)
							mux.WriteFrame(&Frame{Type: FrameTypeClose, ChannelID: sess.channelID})
						}
						delete(sessions, key)
					}
				}
				sessMu.Unlock()
			}
		}
	}()

	buf := make([]byte, 65535)
	for {
		n, remoteAddr, err := pconn.ReadFrom(buf)
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				log.Printf("Read UDP error: %v", err)
				continue
			}
		}

		remoteKey := remoteAddr.String()

		s.clientsMutex.RLock()
		mux := s.clients[clientID]
		s.clientsMutex.RUnlock()

		if mux == nil {
			continue
		}

		sessMu.Lock()
		sess, exists := sessions[remoteKey]
		if exists {
			sess.lastActive = time.Now()
		}
		sessMu.Unlock()

		if !exists {
			channelID, dataChan := mux.OpenChannel()

			if err := mux.WriteFrame(&Frame{
				Type:      FrameTypeOpen,
				ChannelID: channelID,
				Data:      []byte{ProtocolUDP},
			}); err != nil {
				mux.CloseChannel(channelID)
				continue
			}

			sess = &udpSession{
				channelID:  channelID,
				lastActive: time.Now(),
			}

			sessMu.Lock()
			sessions[remoteKey] = sess
			sessMu.Unlock()

			// Handle return traffic from client -> udp public
			go func(chID uint32, dChan chan *Frame, rAddr net.Addr) {
				for frame := range dChan {
					if frame.Type == FrameTypeData && len(frame.Data) > 0 {
						pconn.WriteTo(frame.Data, rAddr)
					} else if frame.Type == FrameTypeClose {
						break
					}
				}
				sessMu.Lock()
				if s, ok := sessions[remoteKey]; ok && s.channelID == chID {
					delete(sessions, remoteKey)
				}
				sessMu.Unlock()
				mux.CloseChannel(chID)
			}(channelID, dataChan, remoteAddr)
		}

		mux.WriteFrame(&Frame{
			Type:      FrameTypeData,
			ChannelID: sess.channelID,
			Data:      buf[:n],
		})
	}
}
