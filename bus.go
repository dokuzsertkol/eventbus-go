package eventbus

import (
	"sync"
	"sync/atomic"
)

type Bus struct {
	events      map[string][]Handler
	queue       chan Event
	workerCount int

	lock         sync.RWMutex
	publishLock  sync.Mutex
	wg           sync.WaitGroup
	panicHandler func(any)

	done   chan struct{}
	closed atomic.Bool
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
	b.publishLock.Lock()
	defer b.publishLock.Unlock()

	if b.closed.Load() {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			b.lock.RLock()
			handler := b.panicHandler
			b.lock.RUnlock()

			handler(r)
		}
	}()

	b.queue <- Event{
		ID:   eventID,
		Data: data,
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
	if b.closed.Swap(true) {
		return
	}

	b.publishLock.Lock()
	close(b.done)
	close(b.queue)
	b.publishLock.Unlock()

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
						b.lock.RLock()
						handler := b.panicHandler
						b.lock.RUnlock()
						handler(r)
					}
				}()

				handler(event)
			}()
		}
	}
}
