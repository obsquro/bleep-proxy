//go:build server
// +build server

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultConfigPath = "server.json"
	readBufferSize    = 32 * 1024
	udpBufferSize     = 65535
	handshakeTimeout  = 10 * time.Second
	keepAlivePeriod   = 30 * time.Second
	cleanupInterval   = 30 * time.Second
	sessionTimeout    = 60 * time.Second
)

type Config struct {
	ClientListenPort string           `json:"client_listen_port"`
	StatsPort        string           `json:"stats_port"`
	Users            []UserConfig     `json:"users"`
	Listeners        []ListenerConfig `json:"listeners"`
}

type UserConfig struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

type ListenerConfig struct {
	Protocol     string `json:"protocol"`
	Port         int    `json:"port"`
	TargetClient string `json:"target_client"`
	Priority     int    `json:"priority"`
}

// --- Interfaces ---

type Authenticator interface {
	Authenticate(id, key string) bool
}

type TunnelRegistry interface {
	Register(id string, tunnel *Multiplexer)
	Unregister(id string)
	Get(id string) *Multiplexer
	Range(f func(id string, tunnel *Multiplexer) bool)
}

type Listener interface {
	Start(ctx context.Context)
}

// --- Authenticator Implementation ---

type InMemoryAuthenticator struct {
	users map[string]string
}

func NewInMemoryAuthenticator(users []UserConfig) *InMemoryAuthenticator {
	m := make(map[string]string)
	for _, u := range users {
		m[u.ID] = u.Key
	}
	return &InMemoryAuthenticator{users: m}
}

func (a *InMemoryAuthenticator) Authenticate(id, key string) bool {
	storedKey, ok := a.users[id]
	return ok && storedKey == key
}


// --- Registry Implementation ---

type InMemoryRegistry struct {
	tunnels map[string]*Multiplexer
	mu      sync.RWMutex
}

func (r *InMemoryRegistry) Range(f func(id string, tunnel *Multiplexer) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, t := range r.tunnels {
		if !f(id, t) {
			break
		}
	}
}

func NewInMemoryRegistry() *InMemoryRegistry {
	return &InMemoryRegistry{
		tunnels: make(map[string]*Multiplexer),
	}
}

func (r *InMemoryRegistry) Register(id string, tunnel *Multiplexer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.tunnels[id]; ok {
		old.Close()
	}
	r.tunnels[id] = tunnel
}

func (r *InMemoryRegistry) Unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tunnels, id)
}

func (r *InMemoryRegistry) Get(id string) *Multiplexer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tunnels[id]
}

type Server struct {
	config   *Config
	auth     Authenticator
	registry TunnelRegistry
	wg       sync.WaitGroup
}

func main() {
	configPath := flag.String("config", defaultConfigPath, "Path to config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	server := NewServer(cfg)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	if err := server.Run(ctx); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	log.Println("Shutdown complete")
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func NewServer(cfg *Config) *Server {
	return &Server{
		config:   cfg,
		auth:     NewInMemoryAuthenticator(cfg.Users),
		registry: NewInMemoryRegistry(),
	}
}

func (s *Server) Run(ctx context.Context) error {
	log.Printf("Starting Bleep Proxy Server")
	
	s.wg.Add(1)
	go s.runStatsServer(ctx)

	s.wg.Add(1)
	go s.runClientListener(ctx)

	for _, lCfg := range s.config.Listeners {
		l, err := NewListenerFactory(lCfg, s.registry)
		if err != nil {
			log.Printf("Failed to create listener for %s:%d: %v", lCfg.Protocol, lCfg.Port, err)
			continue
		}
		s.wg.Add(1)
		go func(l Listener) {
			defer s.wg.Done()
			l.Start(ctx)
		}(l)
	}

	<-ctx.Done()
	s.wg.Wait()
	return nil
}

func (s *Server) runStatsServer(ctx context.Context) {
	defer s.wg.Done()

	mux := http.NewServeMux()
	mux.HandleFunc("/statistics", func(w http.ResponseWriter, r *http.Request) {
		stats := make(map[string]interface{})
		tunnels := make([]map[string]interface{}, 0)

		s.registry.Range(func(id string, t *Multiplexer) bool {
			tunnelStats := map[string]interface{}{
				"client_id":      id,
				"bytes_sent":     atomic.LoadUint64(&t.BytesSent),
				"bytes_received": atomic.LoadUint64(&t.BytesReceived),

			}
			tunnels = append(tunnels, tunnelStats)
			return true
		})

		stats["tunnels_count"] = len(tunnels)
		stats["tunnels"] = tunnels
		stats["listeners"] = s.config.Listeners

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})

	port := s.config.StatsPort
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	log.Printf("Starting Stats Server on :%s", port)

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Printf("Stats server error: %v", err)
	}
}

func (s *Server) runClientListener(ctx context.Context) {
	defer s.wg.Done()
	
	addr := ":" + s.config.ClientListenPort
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("Failed to listen for clients on %s: %v", addr, err)
		return
	}
	defer listener.Close()

	log.Printf("Listening for clients on %s", addr)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("Accept client error: %v", err)
				continue
			}
		}
		go s.handleClientConnection(conn)
	}
}

func (s *Server) handleClientConnection(conn net.Conn) {
    defer func() {
        if r := recover(); r != nil {
            log.Printf("Recovered from panic in client session: %v", r)
        }
    }()
    
    id, key, err := s.handshake(conn)
    if err != nil {
        log.Printf("Handshake failed: %v", err)
        conn.Close()
        return
    }

    if !s.auth.Authenticate(id, key) {
        log.Printf("Authentication failed for %s", id)
        conn.Write([]byte{0}) 
        conn.Close()
        return
    }

    if _, err := conn.Write([]byte{1}); err != nil {
        log.Printf("Failed to send auth success to %s: %v", id, err)
        conn.Close()
        return
    }

    log.Printf("Client connected: %s", id)
    
    mux := NewMultiplexer(conn)
    s.registry.Register(id, mux)

    defer func() {
        s.registry.Unregister(id)
        mux.Close() 
        log.Printf("Client disconnected and cleaned up: %s", id)
    }()

    mux.ReadLoop(func(frame *Frame) {
        if frame.Type == FrameTypeClose {
            mux.CloseChannel(frame.ChannelID)
            log.Printf("Channel %d closed by client %s", frame.ChannelID, id)
        }
    })
}

func (s *Server) handshake(conn net.Conn) (string, string, error) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(keepAlivePeriod)
	}

	conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
	defer conn.SetReadDeadline(time.Time{})

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", "", err
	}

	parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid format")
	}

	return parts[0], parts[1], nil
}

func NewListenerFactory(cfg ListenerConfig, registry TunnelRegistry) (Listener, error) {
	switch cfg.Protocol {
	case "tcp":
		return &TCPListener{
			port:     cfg.Port,
			target:   cfg.TargetClient,
			priority: cfg.Priority,
			registry: registry,
		}, nil
	case "udp":
		return &UDPListener{
			port:     cfg.Port,
			target:   cfg.TargetClient,
			priority: cfg.Priority,
			registry: registry,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", cfg.Protocol)
	}
}

type TCPListener struct {
	port     int
	target   string
	priority int
	registry TunnelRegistry
}

func (l *TCPListener) Start(ctx context.Context) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", l.port))
	if err != nil {
		log.Printf("TCP Listener error on %d: %v", l.port, err)
		return
	}
	defer listener.Close()

	log.Printf("Started TCP Listener on :%d -> %s", l.port, l.target)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("Accept error on :%d: %v", l.port, err)
				continue
			}
		}
		go l.handleConn(conn)
	}
}

func (l *TCPListener) handleConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[TCP] Recovered from panic in handleConn: %v", r)
		}
		conn.Close()
	}()

	tunnel := l.registry.Get(l.target)
	if tunnel == nil {
		log.Printf("Target client %s not connected, dropping TCP connection on :%d", l.target, l.port)
		return
	}

	chID, stream := tunnel.OpenChannel(l.priority)
	log.Printf("[CH %d] New TCP connection on :%d -> %s", chID, l.port, l.target)

	payload := []byte{byte(ProtocolTCP), byte(l.priority)}
	if err := tunnel.WriteFrame(&Frame{Type: FrameTypeOpen, ChannelID: chID, Data: payload}); err != nil {
		tunnel.CloseChannel(chID)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		l.pipeSocketToTunnel(conn, tunnel, chID)
	}()

	go func() {
		defer wg.Done()
		l.pipeTunnelToSocket(stream, conn)
	}()

	wg.Wait()
	tunnel.CloseChannel(chID)
}

func (l *TCPListener) pipeSocketToTunnel(conn net.Conn, tunnel *Multiplexer, chID uint32) {
	buf := make([]byte, readBufferSize)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if err := tunnel.WriteFrame(&Frame{Type: FrameTypeData, ChannelID: chID, Data: data}); err != nil {
				return
			}
		}
		if err != nil {
			tunnel.WriteFrame(&Frame{Type: FrameTypeClose, ChannelID: chID})
			return
		}
	}
}

func (l *TCPListener) pipeTunnelToSocket(stream chan *Frame, conn net.Conn) {
	for frame := range stream {
		if frame.Type != FrameTypeData {
			return
		}
		if _, err := conn.Write(frame.Data); err != nil {
			return
		}
	}
}

type UDPListener struct {
	port     int
	target   string
	priority int
	registry TunnelRegistry
}

func (l *UDPListener) Start(ctx context.Context) {
	conn, err := net.ListenPacket("udp", fmt.Sprintf(":%d", l.port))
	if err != nil {
		log.Printf("UDP Listener error on %d: %v", l.port, err)
		return
	}
	defer conn.Close()

	log.Printf("Started UDP Listener on :%d -> %s", l.port, l.target)

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	sessions := NewUDPSessionManager(l.registry, l.target, l.priority, conn)
	go sessions.CleanupLoop(ctx)

	buf := make([]byte, udpBufferSize)
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("Read error on :%d: %v", l.port, err)
				continue
			}
		}
		
		data := make([]byte, n)
		copy(data, buf[:n])
		sessions.HandlePacket(addr, data)
	}
}

type UDPSessionManager struct {
	registry     TunnelRegistry
	target       string
	priority     int
	conn         net.PacketConn
	sessions     map[string]*UDPSession
	mu           sync.Mutex
}

type UDPSession struct {
	chID       uint32
	lastActive time.Time
}

func NewUDPSessionManager(reg TunnelRegistry, target string, priority int, conn net.PacketConn) *UDPSessionManager {
	return &UDPSessionManager{
		registry: reg,
		target:   target,
		priority: priority,
		conn:     conn,
		sessions: make(map[string]*UDPSession),
	}
}

func (m *UDPSessionManager) CleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.prune()
		}
	}
}

func (m *UDPSessionManager) prune() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for key, s := range m.sessions {
		if now.Sub(s.lastActive) > sessionTimeout {
			if t := m.registry.Get(m.target); t != nil {
				t.CloseChannel(s.chID)
				t.WriteFrame(&Frame{Type: FrameTypeClose, ChannelID: s.chID})
			}
			delete(m.sessions, key)
		}
	}
}

func (m *UDPSessionManager) HandlePacket(addr net.Addr, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[UDP] Recovered from panic in HandlePacket: %v", r)
		}
	}()

	tunnel := m.registry.Get(m.target)
	if tunnel == nil {
		return
	}

	key := addr.String()
	m.mu.Lock()
	sess, exists := m.sessions[key]
	if exists {
		sess.lastActive = time.Now()
	}
	m.mu.Unlock()

	if !exists {
		sess = m.createSession(key, addr, tunnel)
		if sess == nil {
			return
		}
	}

	tunnel.WriteFrame(&Frame{Type: FrameTypeData, ChannelID: sess.chID, Data: data})
}

func (m *UDPSessionManager) createSession(key string, addr net.Addr, tunnel *Multiplexer) *UDPSession {
	chID, stream := tunnel.OpenChannel(m.priority)
	log.Printf("[CH %d] New UDP session %s -> %s", chID, key, m.target)

	payload := []byte{byte(ProtocolUDP), byte(m.priority)}
	if err := tunnel.WriteFrame(&Frame{Type: FrameTypeOpen, ChannelID: chID, Data: payload}); err != nil {
		tunnel.CloseChannel(chID)
		return nil
	}

	sess := &UDPSession{chID: chID, lastActive: time.Now()}
	m.mu.Lock()
	m.sessions[key] = sess
	m.mu.Unlock()

	go func() {
		for frame := range stream {
			if frame.Type == FrameTypeData {
				m.conn.WriteTo(frame.Data, addr)
			} else {
				break
			}
		}
		m.mu.Lock()
		if s, ok := m.sessions[key]; ok && s.chID == chID {
			delete(m.sessions, key)
		}
		m.mu.Unlock()
		tunnel.CloseChannel(chID)
	}()

	return sess
}
