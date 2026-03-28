//go:build server
// +build server

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)



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

type InMemoryAuthenticator struct {
	clients map[string]string
}

func NewInMemoryAuthenticator(clients []ClientAuth) *InMemoryAuthenticator {
	m := make(map[string]string)
	for _, c := range clients {
		m[c.ID] = c.Key
	}
	return &InMemoryAuthenticator{clients: m}
}

func (a *InMemoryAuthenticator) Authenticate(id, key string) bool {
	storedKey, ok := a.clients[id]
	return ok && storedKey == key
}

type InMemoryRegistry struct {
	tunnels map[string]*Multiplexer
	mu      sync.RWMutex
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

func (r *InMemoryRegistry) Range(f func(id string, tunnel *Multiplexer) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, t := range r.tunnels {
		if !f(id, t) {
			break
		}
	}
}

type Server struct {
	config   *ServerConfig
	auth     Authenticator
	registry TunnelRegistry
	wg       sync.WaitGroup
	logger   *Logger
}

func main() {
	configPath := flag.String("config", "server.json", "Path to config file")
	flag.Parse()

	logger := NewLogger("SERVER")

	cfg, err := LoadServerConfig(*configPath)
	if err != nil {
		logger.Fatal("Failed to load config: %v", err)
	}

	logger.Info("Config loaded and validated successfully")

	server := NewServer(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		logger.Info("Shutting down...")
		cancel()
	}()

	if err := server.Run(ctx); err != nil {
		logger.Fatal("Server error: %v", err)
	}
	logger.Info("Shutdown complete")
}



func NewServer(cfg *ServerConfig, logger *Logger) *Server {
	return &Server{
		config:   cfg,
		auth:     NewInMemoryAuthenticator(cfg.Clients),
		registry: NewInMemoryRegistry(),
		logger:   logger,
	}
}

func (s *Server) Run(ctx context.Context) error {
	s.logger.Info("Bleep Proxy Server v%s", Version)

	if s.config.API != "" {
		s.wg.Add(1)
		go s.runStatsServer(ctx)
	}

	s.wg.Add(1)
	go s.runClientListener(ctx)

	for _, lCfg := range s.config.Listeners {
		l, err := NewListenerFactory(lCfg, s.registry, s.logger)
		if err != nil {
			s.logger.Error("Failed to create listener for %s:%d: %v", lCfg.Protocol, lCfg.Port, err)
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
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "healthy",
			"version": Version,
		})
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		type ClientStats struct {
			ID       string `json:"id"`
			Sent     string `json:"sent"`
			Recv     string `json:"recv"`
			Channels int    `json:"channels"`
		}

		type StatsResponse struct {
			Clients   []ClientStats    `json:"clients"`
			Listeners []ListenerConfig `json:"listeners"`
		}

		clients := make([]ClientStats, 0)
		s.registry.Range(func(id string, t *Multiplexer) bool {
			sent, recv, channels := t.GetStats()
			clients = append(clients, ClientStats{
				ID:       id,
				Sent:     FormatBytes(sent),
				Recv:     FormatBytes(recv),
				Channels: channels,
			})
			return true
		})

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(StatsResponse{
			Clients:   clients,
			Listeners: s.config.Listeners,
		})
	})

	server := &http.Server{
		Addr:    ":" + s.config.API,
		Handler: mux,
	}

	s.logger.Info("API Server on :%s", s.config.API)

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		s.logger.Error("Stats server error: %v", err)
	}
}

func (s *Server) runClientListener(ctx context.Context) {
	defer s.wg.Done()

	addr := ":" + s.config.Port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.logger.Fatal("Failed to listen for clients on %s: %v", addr, err)
	}
	defer listener.Close()

	s.logger.Info("Listening for clients on %s", addr)

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
				s.logger.Error("Accept client error: %v", err)
				continue
			}
		}
		go s.handleClientConnection(conn)
	}
}

func (s *Server) handleClientConnection(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("Panic in client session: %v", r)
		}
	}()

	id, key, err := s.handshake(conn)
	if err != nil {
		s.logger.Error("Handshake failed: %v", err)
		conn.Close()
		return
	}

	if !s.auth.Authenticate(id, key) {
		s.logger.Warn("Authentication failed for %s", id)
		conn.Write([]byte{0})
		conn.Close()
		return
	}

	if _, err := conn.Write([]byte{1}); err != nil {
		s.logger.Error("Failed to send auth success to %s: %v", id, err)
		conn.Close()
		return
	}

	s.logger.Info("Client connected: %s", id)

	mux := NewMultiplexer(conn)
	s.registry.Register(id, mux)

	defer func() {
		s.registry.Unregister(id)
		mux.Close()
		s.logger.Info("Client disconnected: %s", id)
	}()

	mux.ReadLoop(func(frame *Frame) {
		if frame.Type == FrameTypeClose {
			mux.CloseChannel(frame.ChannelID)
		}
	})
}

func (s *Server) handshake(conn net.Conn) (string, string, error) {
	if err := ConfigureTCPConn(conn); err != nil {
		s.logger.Warn("Failed to configure TCP: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(HandshakeTimeout))
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

func NewListenerFactory(cfg ListenerConfig, registry TunnelRegistry, logger *Logger) (Listener, error) {
	switch cfg.Protocol {
	case "tcp":
		return &TCPListener{
			port:     cfg.Port,
			target:   cfg.Client,
			priority: cfg.Priority,
			registry: registry,
			logger:   logger,
		}, nil
	case "udp":
		return &UDPListener{
			port:     cfg.Port,
			target:   cfg.Client,
			priority: cfg.Priority,
			registry: registry,
			logger:   logger,
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
	logger   *Logger
}

func (l *TCPListener) Start(ctx context.Context) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", l.port))
	if err != nil {
		l.logger.Error("TCP Listener error on %d: %v", l.port, err)
		return
	}
	defer listener.Close()

	l.logger.Info("TCP Listener on :%d -> %s", l.port, l.target)

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
				l.logger.Error("Accept error on :%d: %v", l.port, err)
				continue
			}
		}
		l.logger.Info("New TCP connection on :%d from %s", l.port, conn.RemoteAddr())
		go l.handleConn(conn)
	}
}

func (l *TCPListener) handleConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			l.logger.Error("Panic in TCP handleConn: %v", r)
		}
		conn.Close()
	}()

	tunnel := l.registry.Get(l.target)
	if tunnel == nil {
		l.logger.Warn("Client %s not connected, dropping TCP connection on :%d", l.target, l.port)
		return
	}

	chID, stream := tunnel.OpenChannel(l.priority)
	l.logger.Info("Opening tunnel: TCP :%d -> %s (CH %d)", l.port, l.target, chID)

	payload := []byte{byte(ProtocolTCP), byte(l.priority)}
	if err := tunnel.WriteFrame(&Frame{
		Type:      FrameTypeOpen,
		ChannelID: chID,
		Data:      payload,
	}); err != nil {
		l.logger.Error("Failed to send OPEN frame for CH %d: %v", chID, err)
		tunnel.CloseChannel(chID)
		return
	}

	l.logger.Debug("OPEN frame sent for CH %d", chID)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		l.pipeSocketToTunnel(conn, tunnel, chID)
		l.logger.Debug("Socket->Tunnel pipe closed for CH %d", chID)
	}()

	go func() {
		defer wg.Done()
		l.pipeTunnelToSocket(stream, conn)
		l.logger.Debug("Tunnel->Socket pipe closed for CH %d", chID)
	}()

	wg.Wait()
	tunnel.CloseChannel(chID)
	l.logger.Info("Tunnel closed: CH %d", chID)
}

func (l *TCPListener) pipeSocketToTunnel(conn net.Conn, tunnel *Multiplexer, chID uint32) {
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
			
			if err := tunnel.WriteFrame(&Frame{
				Type:      FrameTypeData,
				ChannelID: chID,
				Data:      data,
			}); err != nil {
				l.logger.Error("CH %d: Write failed: %v", chID, err)
				return
			}
		}
		
		if err != nil {
			l.logger.Info("CH %d: Closed after %d packets, %d bytes", chID, packetCount, totalBytes)
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
			l.logger.Error("CH %d: Write failed: %v", frame.ChannelID, err)
			return
		}
	}
}

type UDPListener struct {
	port     int
	target   string
	priority int
	registry TunnelRegistry
	logger   *Logger
}

func (l *UDPListener) Start(ctx context.Context) {
	conn, err := net.ListenPacket("udp", fmt.Sprintf(":%d", l.port))
	if err != nil {
		l.logger.Error("UDP Listener error on %d: %v", l.port, err)
		return
	}
	defer conn.Close()

	l.logger.Info("UDP Listener on :%d -> %s", l.port, l.target)

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	sessions := NewUDPSessionManager(l.registry, l.target, l.priority, conn, l.logger)
	go sessions.CleanupLoop(ctx)

	buf := make([]byte, UDPBufferSize)
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				l.logger.Error("Read error on :%d: %v", l.port, err)
				continue
			}
		}

		data := make([]byte, n)
		copy(data, buf[:n])
		sessions.HandlePacket(addr, data)
	}
}

type UDPSessionManager struct {
	registry TunnelRegistry
	target   string
	priority int
	conn     net.PacketConn
	sessions map[string]*UDPSession
	mu       sync.Mutex
	logger   *Logger
}

type UDPSession struct {
	chID       uint32
	lastActive time.Time
}

func NewUDPSessionManager(reg TunnelRegistry, target string, priority int, conn net.PacketConn, logger *Logger) *UDPSessionManager {
	return &UDPSessionManager{
		registry: reg,
		target:   target,
		priority: priority,
		conn:     conn,
		sessions: make(map[string]*UDPSession),
		logger:   logger,
	}
}

func (m *UDPSessionManager) CleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(CleanupInterval)
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
		if now.Sub(s.lastActive) > SessionTimeout {
			if t := m.registry.Get(m.target); t != nil {
				t.CloseChannel(s.chID)
				t.WriteFrame(&Frame{Type: FrameTypeClose, ChannelID: s.chID})
			}
			delete(m.sessions, key)
			m.logger.Debug("UDP session %s pruned", key)
		}
	}
}

func (m *UDPSessionManager) HandlePacket(addr net.Addr, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("Panic in UDP HandlePacket: %v", r)
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

	tunnel.WriteFrame(&Frame{
		Type:      FrameTypeData,
		ChannelID: sess.chID,
		Data:      data,
	})
}

func (m *UDPSessionManager) createSession(key string, addr net.Addr, tunnel *Multiplexer) *UDPSession {
	chID, stream := tunnel.OpenChannel(m.priority)
	m.logger.Debug("New UDP session %s -> %s (CH %d)", key, m.target, chID)

	payload := []byte{byte(ProtocolUDP), byte(m.priority)}
	if err := tunnel.WriteFrame(&Frame{
		Type:      FrameTypeOpen,
		ChannelID: chID,
		Data:      payload,
	}); err != nil {
		tunnel.CloseChannel(chID)
		return nil
	}

	sess := &UDPSession{
		chID:       chID,
		lastActive: time.Now(),
	}

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
