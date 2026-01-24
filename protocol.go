package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

const (
	FrameTypeData  byte = 0x01
	FrameTypeOpen  byte = 0x02
	FrameTypeClose byte = 0x03

	ProtocolTCP byte = 0x00
	ProtocolUDP byte = 0x01
)

type Frame struct {
	Type      byte
	ChannelID uint32
	Data      []byte
}

func (f *Frame) WriteTo(w io.Writer) error {
	header := make([]byte, 9)
	header[0] = f.Type
	binary.BigEndian.PutUint32(header[1:5], f.ChannelID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(f.Data)))

	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(f.Data) > 0 {
		if _, err := w.Write(f.Data); err != nil {
			return err
		}
	}
	return nil
}

func ReadFrame(r io.Reader) (*Frame, error) {
	header := make([]byte, 9)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	frame := &Frame{
		Type:      header[0],
		ChannelID: binary.BigEndian.Uint32(header[1:5]),
	}

	dataLen := binary.BigEndian.Uint32(header[5:9])
	if dataLen > 0 {
		frame.Data = make([]byte, dataLen)
		if _, err := io.ReadFull(r, frame.Data); err != nil {
			return nil, err
		}
	}

	return frame, nil
}

type Multiplexer struct {
	conn      net.Conn
	channels  map[uint32]chan *Frame
	mu        sync.RWMutex
	writeMu   sync.Mutex
	nextID    uint32
	closed    bool
	closedChs map[uint32]bool
}

func NewMultiplexer(conn net.Conn) *Multiplexer {
	return &Multiplexer{
		conn:      conn,
		channels:  make(map[uint32]chan *Frame),
		closedChs: make(map[uint32]bool),
		nextID:    1,
	}
}

func (m *Multiplexer) WriteFrame(frame *Frame) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	m.mu.RLock()
	closed := m.closed
	m.mu.RUnlock()

	if closed {
		return fmt.Errorf("multiplexer closed")
	}

	return frame.WriteTo(m.conn)
}

func (m *Multiplexer) ReadLoop(handler func(*Frame)) {
	for {
		frame, err := ReadFrame(m.conn)
		if err != nil {
			m.Close()
			return
		}
		handler(frame)
	}
}

func (m *Multiplexer) OpenChannel() (uint32, chan *Frame) {
	m.mu.Lock()
	defer m.mu.Unlock()

	channelID := m.nextID
	m.nextID++

	ch := make(chan *Frame, 10)
	m.channels[channelID] = ch

	return channelID, ch
}

func (m *Multiplexer) RegisterChannel(channelID uint32) chan *Frame {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan *Frame, 10)
	m.channels[channelID] = ch

	return ch
}

func (m *Multiplexer) GetChannel(channelID uint32) (chan *Frame, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ch, exists := m.channels[channelID]
	return ch, exists
}

func (m *Multiplexer) CloseChannel(channelID uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closedChs[channelID] {
		return
	}

	if ch, exists := m.channels[channelID]; exists {
		m.closedChs[channelID] = true
		close(ch)
		delete(m.channels, channelID)
	}
}

func (m *Multiplexer) SafeSend(channelID uint32, frame *Frame) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed || m.closedChs[channelID] {
		return false
	}

	ch, exists := m.channels[channelID]
	if !exists {
		return false
	}

	select {
	case ch <- frame:
		return true
	default:
		return false
	}
}

func (m *Multiplexer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return
	}
	m.closed = true

	for _, ch := range m.channels {
		close(ch)
	}
	m.channels = make(map[uint32]chan *Frame)
	m.conn.Close()
}
