# eventbus-go

A lightweight, concurrent event bus library for Go with worker pool support and panic recovery.

## Features

- **Concurrent Event Publishing** - Async event handling with configurable worker pool
- **Panic Recovery** - Built-in panic handling with custom recovery functions
- **Multiple Handlers** - Register multiple handlers for the same event
- **Thread-Safe** - Safe concurrent access with sync.RWMutex
- **Flexible Data Types** - Support for any data type via `interface{}`

## Installation

```bash
go get github.com/dokuzsertkol/eventbus-go
```

## Quick Start

### Basic Usage

```go
package main

import (
	"fmt"
	"github.com/dokuzsertkol/eventbus-go"
)

func main() {
	// Create a new bus with 2 workers and queue size of 10
	bus := eventbus.New(2, 10)
	defer bus.Close()

	// Register an event handler
	bus.Register("greeting", func(e eventbus.Event) {
		fmt.Println("Hello:", e.Data)
	})

	// Publish an event
	bus.Publish("greeting", "World")
}
```

## API

### `New(workerCount int, queueSize int) *Bus`

Creates a new event bus instance.

- `workerCount`: Number of concurrent workers to process events
- `queueSize`: Size of the event queue buffer

```go
bus := eventbus.New(4, 100)
```

### `Register(eventID string, handler Handler)`

Registers a handler for a specific event ID. Multiple handlers can be registered for the same event.

```go
bus.Register("user.created", func(e eventbus.Event) {
	user := e.Data.(User)
	fmt.Printf("User created: %s\n", user.Name)
})
```

### `Publish(eventID string, data any)`

Publishes an event to the bus. The publish operation is non-blocking.

```go
bus.Publish("user.created", User{ID: 1, Name: "John"})
```

### `SetPanicHandler(handler func(any))`

Sets a custom panic handler to recover from panics in event handlers.

```go
bus.SetPanicHandler(func(r any) {
	log.Printf("Panic recovered: %v\n", r)
})
```

### `Close()`

Closes the event bus and waits for all events to be processed.

```go
defer bus.Close()
```

## Examples

### Handling Multiple Event Types

```go
bus := eventbus.New(2, 10)
defer bus.Close()

// String handler
bus.Register("greeting", func(e eventbus.Event) {
	fmt.Println("Message:", e.Data.(string))
})

// Integer handler
bus.Register("counter", func(e eventbus.Event) {
	fmt.Println("Count:", e.Data.(int))
})

bus.Publish("greeting", "Hello")
bus.Publish("counter", 42)
```

### Custom Types

```go
type User struct {
	ID   int
	Name string
}

bus.Register("user.signup", func(e eventbus.Event) {
	user := e.Data.(User)
	fmt.Printf("New user: %s (ID: %d)\n", user.Name, user.ID)
})

bus.Publish("user.signup", User{ID: 1, Name: "Alice"})
```

### Type Switch Pattern

```go
bus.Register("universal", func(e eventbus.Event) {
	switch v := e.Data.(type) {
	case string:
		fmt.Println("String:", v)
	case int:
		fmt.Println("Integer:", v)
	case User:
		fmt.Println("User:", v.Name)
	default:
		fmt.Println("Unknown type:", v)
	}
})
```

### Error Handling

```go
bus.SetPanicHandler(func(r any) {
	log.Printf("Panic in event handler: %v\n", r)
})

bus.Register("risky", func(e eventbus.Event) {
	// Panic will be caught by the panic handler
	panic("something went wrong")
})
```

## How It Works

1. **Event Registration** - Handlers are registered for specific event IDs
2. **Publishing** - Events are published to a buffered channel
3. **Processing** - Worker goroutines process events from the queue
4. **Execution** - All handlers for an event are executed sequentially
5. **Panic Recovery** - Any panic in a handler is caught and passed to the panic handler

## Thread Safety

The event bus is fully thread-safe:
- Event registration and publishing are protected by `sync.RWMutex`
- Worker goroutines safely process events from a shared channel
- All operations can be called concurrently from multiple goroutines

## Testing

Run the test suite:

```bash
go test -v
```

## Configuration Tips

### Worker Count
- Use more workers for I/O-bound operations
- Use fewer workers (1-2) for CPU-bound operations
- Start with `runtime.NumCPU()` for optimal performance

### Queue Size
- Larger queue = more memory but better for burst traffic
- Smaller queue = lower memory but may drop events if full
- Choose based on your event publishing patterns

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the GNU General Public License v3.0 (GPLv3).
See the LICENSE file for details.

## Author

[dokuzsertkol](https://github.com/dokuzsertkol)
