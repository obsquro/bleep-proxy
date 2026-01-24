package main

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/pierrec/lz4/v4"
)

const (
	FrameTypeData           byte = 0x01
	FrameTypeOpen           byte = 0x02
	FrameTypeClose          byte = 0x03
	FrameTypeCompressedData byte = 0x04

	ProtocolTCP Protocol = 0x00
	ProtocolUDP Protocol = 0x01

	ChannelBufferSize = 128
	QueueSize         = 1024
	CompressionThresh = 512
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 65535) 
		return &b
	},
}

func GetBuffer() *[]byte {
	return bufferPool.Get().(*[]byte)
}

func PutBuffer(b *[]byte) {
	bufferPool.Put(b)
}

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
		if frame.Type != FrameTypeCompressedData {
			frame.Data = make([]byte, dataLen)
			if _, err := io.ReadFull(r, frame.Data); err != nil {
				return nil, err
			}
		} else {
			compressed := make([]byte, dataLen)
			if _, err := io.ReadFull(r, compressed); err != nil {
				return nil, err
			}
			
			decompressed := make([]byte, 65535)
			n, err := lz4.UncompressBlock(compressed, decompressed)
			if err != nil {
				return nil, fmt.Errorf("decompression error: %v", err)
			}
			frame.Data = decompressed[:n]
			frame.Type = FrameTypeData
		}
	}

	return frame, nil
}

type Multiplexer struct {
	conn      net.Conn
	channels  map[uint32]chan *Frame
	mu        sync.RWMutex

	outQueue     PacketHeap
	queueCond    *sync.Cond
	queueMu      sync.Mutex
	chanPriority map[uint32]int
	prioMu       sync.RWMutex

	nextID     uint32
	writeOrder uint64
	closed     bool
	closedChs  map[uint32]bool

	BytesSent     uint64
	BytesReceived uint64
	ActiveTunnels int64
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
	compressBuffer := make([]byte, 65535)

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

		if frame.Type == FrameTypeData && len(frame.Data) > CompressionThresh {
			ht := make([]int, 65536)
			n, err := lz4.CompressBlock(frame.Data, compressBuffer, ht)
			if err == nil && n < len(frame.Data) {
				compressedData := make([]byte, n)
				copy(compressedData, compressBuffer[:n])
				frame.Data = compressedData
				frame.Type = FrameTypeCompressedData
			}
		}

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
			ch <- frame
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

	m.queueMu.Lock()
	m.closed = true
	m.queueCond.Broadcast()
	m.queueMu.Unlock()

	for _, ch := range m.channels {
		close(ch)
	}
	m.channels = make(map[uint32]chan *Frame)
	m.conn.Close()
}
