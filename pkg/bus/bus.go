package bus

import (
	"sync"
	"time"
)

type DeliveryMode int

const (
	ModeVolatile DeliveryMode = iota
	ModeDurable
)

// Event is the standard unit of data moving through Siphon
type Event struct {
	ID        uint64
	Topic     string
	Payload   []byte
	Mode      DeliveryMode
	Timestamp time.Time

	Ack  func()
	Nack func()
}

// Bus defines the interface for publishing and subscribing
type Bus interface {
	Publish(topic string, payload []byte) error
	Subscribe(topic string) <-chan Event
}

// MemoryBus is a high-speed, volatile event bus using Go channels
type MemoryBus struct {
	subscribers map[string][]chan Event
	lock        sync.RWMutex
	seqCounter  uint64
}

func NewMemoryBus() *MemoryBus {
	return &MemoryBus{
		subscribers: make(map[string][]chan Event),
	}
}

// Publish sends data to all subscribers of a topic without blocking
func (b *MemoryBus) Publish(topic string, payload []byte) error {
	b.lock.Lock()
	b.seqCounter++
	id := b.seqCounter
	b.lock.Unlock()

	event := Event{
		ID:        id,
		Topic:     topic,
		Payload:   payload,
		Mode:      ModeVolatile,
		Timestamp: time.Now(),
		Ack:       func() {}, // No-op for volatile mode
		Nack:      func() {},
	}

	b.lock.RLock()
	subs := b.subscribers[topic]
	b.lock.RUnlock()

	for _, ch := range subs {
		// Non-blocking Ring Buffer logic
		select {
		case ch <- event:
		default:
			// Channel full: drop oldest unread message to make room for newest
			select {
			case <-ch:
			default:
			}
			// Push new event
			select {
			case ch <- event:
			default:
			}
		}
	}
	return nil
}

// Subscribe returns a channel that receives events for a specific topic
func (b *MemoryBus) Subscribe(topic string) <-chan Event {
	b.lock.Lock()
	defer b.lock.Unlock()

	ch := make(chan Event, 1) // Buffer size 1 for simple backpressure handling
	b.subscribers[topic] = append(b.subscribers[topic], ch)
	return ch
}
