package eventbus

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	bus := New(2, 10)
	if bus == nil {
		t.Fatal("bus is nil")
	}
	defer bus.Close()
}

func TestRegisterAndPublish(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var called bool
	var mu sync.Mutex

	bus.Register("test", func(e Event) {
		mu.Lock()
		called = true
		mu.Unlock()
	})

	bus.Publish("test", nil)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !called {
		t.Error("handler not called")
	}
	mu.Unlock()
}

func TestMultipleHandlers(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var counter int32

	bus.Register("test", func(e Event) {
		atomic.AddInt32(&counter, 1)
	})
	bus.Register("test", func(e Event) {
		atomic.AddInt32(&counter, 1)
	})

	bus.Publish("test", nil)
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&counter) != 2 {
		t.Errorf("expected 2 calls, got %d", counter)
	}
}

func TestDifferentEventIDs(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var event1Called, event2Called bool
	var mu sync.Mutex

	bus.Register("event1", func(e Event) {
		mu.Lock()
		event1Called = true
		mu.Unlock()
	})
	bus.Register("event2", func(e Event) {
		mu.Lock()
		event2Called = true
		mu.Unlock()
	})

	bus.Publish("event1", nil)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !event1Called {
		t.Error("event1 handler not called")
	}
	if event2Called {
		t.Error("event2 handler called unexpectedly")
	}
	mu.Unlock()
}

func TestDataPassing(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	expected := "test data"
	var received string
	var mu sync.Mutex

	bus.Register("test", func(e Event) {
		mu.Lock()
		received = e.Data.(string)
		mu.Unlock()
	})

	bus.Publish("test", expected)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if received != expected {
		t.Errorf("expected %s, got %s", expected, received)
	}
	mu.Unlock()
}

func TestPanicRecovery(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var panicCalled bool
	var mu sync.Mutex

	bus.SetPanicHandler(func(r any) {
		mu.Lock()
		panicCalled = true
		mu.Unlock()
	})

	bus.Register("panic", func(e Event) {
		panic("test panic")
	})

	bus.Publish("panic", nil)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !panicCalled {
		t.Error("panic handler not called")
	}
	mu.Unlock()
}

func TestPanicHandlerPropagation(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var mu sync.Mutex
	var panicValue any

	bus.SetPanicHandler(func(r any) {
		mu.Lock()
		panicValue = r
		mu.Unlock()
	})

	bus.Register("panic", func(e Event) {
		panic("custom error message")
	})

	bus.Publish("panic", nil)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if panicValue != "custom error message" {
		t.Errorf("expected 'custom error message', got %v", panicValue)
	}
	mu.Unlock()
}

func TestMultiplePanics(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var panicCount int32
	bus.SetPanicHandler(func(r any) {
		atomic.AddInt32(&panicCount, 1)
	})

	bus.Register("panic", func(e Event) {
		panic("test")
	})

	for i := 0; i < 5; i++ {
		bus.Publish("panic", nil)
	}
	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&panicCount) != 5 {
		t.Errorf("expected 5 panics, got %d", panicCount)
	}
}

func TestGracefulShutdown(t *testing.T) {
	bus := New(2, 10)

	var processed int32
	bus.Register("test", func(e Event) {
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&processed, 1)
	})

	for i := 0; i < 10; i++ {
		bus.Publish("test", i)
	}

	bus.Close()

	if atomic.LoadInt32(&processed) != 10 {
		t.Errorf("expected 10 processed, got %d", processed)
	}
}

func TestPublishAfterClose(t *testing.T) {
	bus := New(2, 10)

	var called bool
	bus.Register("test", func(e Event) {
		called = true
	})

	bus.Close()
	bus.Publish("test", nil)
	time.Sleep(100 * time.Millisecond)

	if called {
		t.Error("handler called after bus closed")
	}
}

func TestRegisterAfterClose(t *testing.T) {
	bus := New(2, 10)
	bus.Close()

	// should not panic
	bus.Register("test", func(e Event) {})
}

func TestMultipleClose(t *testing.T) {
	bus := New(2, 10)
	bus.Close()
	bus.Close() // second close should be safe
	bus.Close() // third close should be safe
}
func TestPublishDuringClose(t *testing.T) {
	bus := New(5, 10)

	var wg sync.WaitGroup

	// publishers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bus.Publish("test", "data")
		}()
	}

	bus.Close()
	wg.Wait()

	// should not panic or deadlock
}

func TestPendingEventsAfterClose(t *testing.T) {
	bus := New(1, 5)
	var processed int32

	bus.Register("test", func(e Event) {
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&processed, 1)
	})

	// fill queue
	for i := 0; i < 5; i++ {
		bus.Publish("test", i)
	}

	bus.Close()

	if atomic.LoadInt32(&processed) != 5 {
		t.Errorf("expected 5, got %d", processed)
	}
}

func TestConcurrentPublish(t *testing.T) {
	bus := New(5, 100)
	defer bus.Close()

	var counter int32
	bus.Register("test", func(e Event) {
		atomic.AddInt32(&counter, 1)
	})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bus.Publish("test", nil)
		}()
	}
	wg.Wait()

	time.Sleep(500 * time.Millisecond)

	if atomic.LoadInt32(&counter) != 100 {
		t.Errorf("expected 100, got %d", counter)
	}
}

func TestConcurrentRegister(t *testing.T) {
	bus := New(5, 100)
	defer bus.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			bus.Register("test", func(e Event) {})
		}(i)
	}
	wg.Wait()
}

func TestConcurrentPublishAndClose(t *testing.T) {
	bus := New(5, 100)

	var wg sync.WaitGroup

	// publishers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				bus.Publish("test", j)
			}
		}()
	}

	// close in the middle
	time.Sleep(50 * time.Millisecond)
	bus.Close()

	wg.Wait()
	// should not panic
}

func TestConcurrentSetPanicHandler(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bus.SetPanicHandler(func(r any) {})
		}()
	}
	wg.Wait()
}

func TestZeroWorker(t *testing.T) {
	bus := New(0, 10)
	defer bus.Close()

	var called bool
	bus.Register("test", func(e Event) {
		called = true
	})

	bus.Publish("test", nil)
	time.Sleep(100 * time.Millisecond)

	if called {
		t.Error("handler should not be called with 0 workers")
	}
}

func TestZeroQueueSize(t *testing.T) {
	bus := New(2, 0)
	defer bus.Close()

	var mu sync.Mutex
	var called bool

	bus.Register("test", func(e Event) {
		mu.Lock()
		called = true
		mu.Unlock()
	})

	bus.Publish("test", nil)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !called {
		t.Error("handler not called")
	}
	mu.Unlock()
}

func TestDifferentDataTypes(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var results []string
	var mu sync.Mutex

	bus.Register("printer", func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		switch v := e.Data.(type) {
		case string:
			results = append(results, "string:"+v)
		case int:
			results = append(results, "int:"+string(rune(v)))
		case bool:
			results = append(results, "bool:"+string(rune(0)))
		}
	})

	bus.Publish("printer", "hello")
	bus.Publish("printer", 42)
	bus.Publish("printer", true)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	mu.Unlock()
}

func TestNilData(t *testing.T) {
	bus := New(2, 10)
	defer bus.Close()

	var mu sync.Mutex
	var received any

	bus.Register("test", func(e Event) {
		mu.Lock()
		received = e.Data
		mu.Unlock()
	})

	bus.Publish("test", nil)
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if received != nil {
		t.Errorf("expected nil, got %v", received)
	}
	mu.Unlock()
}

func BenchmarkPublish(b *testing.B) {
	bus := New(10, 1000)
	defer bus.Close()

	bus.Register("test", func(e Event) {})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("test", i)
	}
}

func BenchmarkPublishWithHandler(b *testing.B) {
	bus := New(10, 1000)
	defer bus.Close()

	bus.Register("test", func(e Event) {
		_ = e.Data.(int)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("test", i)
	}
}

func BenchmarkConcurrentPublish(b *testing.B) {
	bus := New(10, 1000)
	defer bus.Close()

	bus.Register("test", func(e Event) {})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bus.Publish("test", nil)
		}
	})
}

func BenchmarkPublishWithPanicHandler(b *testing.B) {
	bus := New(10, 1000)
	defer bus.Close()

	bus.SetPanicHandler(func(r any) {})
	bus.Register("test", func(e Event) {})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish("test", i)
	}
}
