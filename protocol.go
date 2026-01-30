package main

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// Frame types
const (
	FrameTypeData           byte = 0x01
	FrameTypeOpen           byte = 0x02
	FrameTypeClose          byte = 0x03
	FrameTypeCompressedData byte = 0x04
)

// Supported protocols
const (
	ProtocolTCP Protocol = 0x00
	ProtocolUDP Protocol = 0x01
)

const (
	ChannelBufferSize = 128
	QueueSize         = 1024
)

type PacketHeap []*Frame

func (h PacketHeap) Len() int { return len(h) }
func (h PacketHeap) Less(i, j int) bool {
	if h[i].Priority == h[j].Priority {
		return h[i].Order < h[j].Order 
	}
	return h[i].Priority > h[j].Priority
}
func (h PacketHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *PacketHeap) Push(x interface{}) {
	*h = append(*h, x.(*Frame))
}

func (h *PacketHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Protocol byte

func (p Protocol) String() string {
	switch p {
	case ProtocolUDP:
		return "udp"
	default:
		return "tcp"
	}
}

type Frame struct {
	Type      byte
	ChannelID uint32
	Priority  int
	Order     uint64
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
		// Limit data length to avoid OOM
		if dataLen > 10*1024*1024 {
			return nil, fmt.Errorf("frame data too large: %d", dataLen)
		}
		frame.Data = make([]byte, dataLen)
		if _, err := io.ReadFull(r, frame.Data); err != nil {
			return nil, err
		}
	}
	return frame, nil
}

type Multiplexer struct {
	conn      net.Conn
	mu        sync.RWMutex
	channels  map[uint32]chan *Frame
	closedChs map[uint32]bool
	closed    bool

	// Priority queue for outgoing frames
	queueMu      sync.Mutex
	queueCond    *sync.Cond
	outQueue     PacketHeap
	writeOrder   uint64
	chanPriority map[uint32]int
	prioMu       sync.RWMutex

	nextID uint32

	// Statistics
	BytesSent     uint64
	BytesReceived uint64
}

func NewMultiplexer(conn net.Conn) *Multiplexer {
	m := &Multiplexer{
		conn:         conn,
		channels:     make(map[uint32]chan *Frame),
		closedChs:    make(map[uint32]bool),
		chanPriority: make(map[uint32]int),
		outQueue:     make(PacketHeap, 0),
		nextID:       1,
	}
	m.queueCond = sync.NewCond(&m.queueMu)
	go m.writeLoop()
	return m
}

func (m *Multiplexer) writeLoop() {
	for {
		m.queueMu.Lock()
		for m.outQueue.Len() == 0 {
			if m.closed {
				m.queueMu.Unlock()
				return
			}
			m.queueCond.Wait()
		}

		frame := heap.Pop(&m.outQueue).(*Frame)
		m.queueMu.Unlock()

		if err := frame.WriteTo(m.conn); err != nil {
			m.Close()
			return
		}
		atomic.AddUint64(&m.BytesSent, uint64(len(frame.Data)+9))
	}
}

func (m *Multiplexer) WriteFrame(frame *Frame) error {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return fmt.Errorf("multiplexer closed")
	}
	m.mu.RUnlock()

	if frame.Type == FrameTypeOpen || frame.Type == FrameTypeClose {
		frame.Priority = 999999
	} else {
		m.prioMu.RLock()
		if p, ok := m.chanPriority[frame.ChannelID]; ok {
			frame.Priority = p
		}
		m.prioMu.RUnlock()
	}

	m.queueMu.Lock()
	frame.Order = m.writeOrder
	m.writeOrder++
	heap.Push(&m.outQueue, frame)
	m.queueCond.Signal()
	m.queueMu.Unlock()

	return nil
}

func (m *Multiplexer) ReadLoop(handler func(*Frame)) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Mux] Recovered from panic in ReadLoop: %v", r)
		}
	}()

    for {
        frame, err := ReadFrame(m.conn)
        if err != nil {
            m.Close()
            return
        }
        atomic.AddUint64(&m.BytesReceived, uint64(len(frame.Data)+9))

        m.mu.RLock()
        ch, exists := m.channels[frame.ChannelID]
        m.mu.RUnlock()

        if exists && (frame.Type == FrameTypeData || frame.Type == FrameTypeClose || frame.Type == FrameTypeCompressedData) {
            func() {
                defer func() { recover() }()
                
                select {
				case ch <- frame:
				default:
					// Log dropped frames if necessary, but keep it quiet for now
				}
            }()
        } else {
            handler(frame)
        }
    }
}

func (m *Multiplexer) OpenChannel(priority int) (uint32, chan *Frame) {
	m.mu.Lock()
	defer m.mu.Unlock()

	channelID := m.nextID
	m.nextID++

	ch := make(chan *Frame, ChannelBufferSize)
	m.channels[channelID] = ch

	m.prioMu.Lock()
	m.chanPriority[channelID] = priority
	m.prioMu.Unlock()

	return channelID, ch
}

func (m *Multiplexer) RegisterChannel(channelID uint32, priority int) chan *Frame {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan *Frame, ChannelBufferSize)
	m.channels[channelID] = ch
	
	m.prioMu.Lock()
	m.chanPriority[channelID] = priority
	m.prioMu.Unlock()

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

    ch, exists := m.channels[channelID]
    if !exists {
        return
    }

    delete(m.channels, channelID)
    m.closedChs[channelID] = true
    
    go func(c chan *Frame) {
        defer func() { recover() }()
        close(c)
    }(ch)
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
    if m.closed {
        m.mu.Unlock()
        return
    }
    m.closed = true
    
    channelsToClose := m.channels
    m.channels = make(map[uint32]chan *Frame)
    m.mu.Unlock()

    m.queueMu.Lock()
    m.queueCond.Broadcast()
    m.queueMu.Unlock()

    m.conn.Close()

    for _, ch := range channelsToClose {
        func() {
            defer func() { recover() }()
            close(ch)
        }()
    }
}
