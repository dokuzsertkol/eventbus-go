package eventbus

import (
	"sync"
)

type Bus struct {
	events      map[string][]Handler
	queue       chan Event
	workerCount int

	lock         sync.RWMutex
	wg           sync.WaitGroup
	panicHandler func(any)

	done chan struct{}
}

func New(workerCount int, queueSize int) *Bus {
	b := &Bus{
		events:       map[string][]Handler{},
		queue:        make(chan Event, queueSize),
		workerCount:  workerCount,
		done:         make(chan struct{}),
		panicHandler: func(any) {},
	}

	for range workerCount {
		b.wg.Add(1)
		go b.worker()
	}

	return b
}

func (b *Bus) Publish(eventID string, data any) {
	defer func() {
		if r := recover(); r != nil {
			b.panicHandler(r)
		}
	}()

	select {
	case <-b.done:
		return
	case b.queue <- Event{
		ID:   eventID,
		Data: data,
	}:
	}
}

func (b *Bus) Register(eventID string, handler Handler) {
	b.lock.Lock()
	defer b.lock.Unlock()

	select {
	case <-b.done:
		return
	default:
		b.events[eventID] = append(b.events[eventID], handler)
	}
}

func (b *Bus) SetPanicHandler(handler func(any)) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.panicHandler = handler
}

func (b *Bus) Close() {
	select {
	case <-b.done:
		return
	default:
		close(b.done)
	}

	close(b.queue)
	b.wg.Wait()
}

func (b *Bus) worker() {
	defer b.wg.Done()

	for event := range b.queue {
		b.lock.RLock()
		handlers := b.events[event.ID]
		b.lock.RUnlock()

		for _, handler := range handlers {
			func() {
				defer func() {
					if r := recover(); r != nil {
						b.panicHandler(r)
					}
				}()

				handler(event)
			}()
		}
	}
}
