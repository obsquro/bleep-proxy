package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

const (
	Version = "1.0.0"

	ReadBufferSize    = 32 * 1024
	UDPBufferSize     = 65535
	DialTimeout       = 10 * time.Second
	LocalDialTimeout  = 5 * time.Second
	HandshakeTimeout  = 10 * time.Second
	KeepAlivePeriod   = 30 * time.Second
	RetryInterval     = 5 * time.Second
	ShutdownTimeout   = 3 * time.Second
	CleanupInterval   = 30 * time.Second
	SessionTimeout    = 60 * time.Second
)

type LogLevel int

const (
	LogDebug LogLevel = iota
	LogInfo
	LogWarn
	LogError
)

type Logger struct {
	prefix string
	level  LogLevel
}

func NewLogger(prefix string) *Logger {
	return &Logger{
		prefix: prefix,
		level:  LogInfo,
	}
}

func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= LogDebug {
		log.Printf("[%s] [DEBUG] "+format, append([]interface{}{l.prefix}, args...)...)
	}
}

func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= LogInfo {
		log.Printf("[%s] [INFO] "+format, append([]interface{}{l.prefix}, args...)...)
	}
}

func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level <= LogWarn {
		log.Printf("[%s] [WARN] "+format, append([]interface{}{l.prefix}, args...)...)
	}
}

func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= LogError {
		log.Printf("[%s] [ERROR] "+format, append([]interface{}{l.prefix}, args...)...)
	}
}

func (l *Logger) Fatal(format string, args ...interface{}) {
	log.Fatalf("[%s] [FATAL] "+format, append([]interface{}{l.prefix}, args...)...)
}

func ConfigureTCPConn(conn net.Conn) error {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("not a TCP connection")
	}

	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}
	if err := tcpConn.SetKeepAlivePeriod(KeepAlivePeriod); err != nil {
		return err
	}
	if err := tcpConn.SetNoDelay(true); err != nil {
		return err
	}

	return nil
}

func FormatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

type UDPConnWrapper struct {
	net.PacketConn
	target net.Addr
}

func (w *UDPConnWrapper) Read(b []byte) (int, error) {
	n, _, err := w.ReadFrom(b)
	return n, err
}

func (w *UDPConnWrapper) Write(b []byte) (int, error) {
	return w.WriteTo(b, w.target)
}

func (w *UDPConnWrapper) LocalAddr() net.Addr              { return w.PacketConn.LocalAddr() }
func (w *UDPConnWrapper) RemoteAddr() net.Addr             { return w.target }
func (w *UDPConnWrapper) SetDeadline(t time.Time) error    { return w.PacketConn.SetDeadline(t) }
func (w *UDPConnWrapper) SetReadDeadline(t time.Time) error  { return w.PacketConn.SetReadDeadline(t) }
func (w *UDPConnWrapper) SetWriteDeadline(t time.Time) error { return w.PacketConn.SetWriteDeadline(t) }
