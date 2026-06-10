# eventbus-go

A lightweight, concurrent event bus library for Go with generic event IDs, worker pool support, and panic recovery.

## Features

* **Generic Event IDs** - Use any `comparable` type as an event identifier
* **Concurrent Event Publishing** - Async event handling with configurable worker pool
* **Panic Recovery** - Built-in panic handling with custom recovery functions
* **Multiple Handlers** - Register multiple handlers for the same event
* **Thread-Safe** - Safe concurrent access with `sync.RWMutex`
* **Flexible Data Types** - Support for any payload type via `any`
* **Type-Safe Events** - Avoid string-based event name typos with enums and custom types

## Installation

```bash
go get github.com/dokuzsertkol/eventbus-go
```

## Why Generics?

Unlike traditional event bus implementations that rely on string-based event names, `eventbus-go` allows any `comparable` type to be used as an event identifier.

Benefits include:

* Compile-time type safety
* IDE autocomplete support
* Elimination of string typos
* Support for enums (`iota`)
* Support for custom event identifier types

## Quick Start

### Basic Usage

```go
package main

import (
	"fmt"

	"github.com/dokuzsertkol/eventbus-go"
)

type EventID string

const (
	EventGreeting EventID = "greeting"
)

func main() {
	bus := eventbus.New[EventID](2, 10)
	defer bus.Close()

	bus.Register(EventGreeting, func(e eventbus.Event[EventID]) {
		fmt.Println("Hello:", e.Data)
	})

	bus.Publish(EventGreeting, "World")
}
```

## API

### `New[E comparable](workerCount int, queueSize int) *Bus[E]`

Creates a new event bus instance.

Parameters:

* `workerCount` - Number of worker goroutines processing events
* `queueSize` - Size of the internal buffered event queue

```go
bus := eventbus.New[EventID](4, 100)
```

### `Register(eventID E, handler Handler[E])`

Registers a handler for a specific event identifier.

Multiple handlers may be registered for the same event.

```go
type EventID string

const (
	EventUserCreated EventID = "user.created"
)

bus.Register(EventUserCreated, func(e eventbus.Event[EventID]) {
	user := e.Data.(User)

	fmt.Printf("User created: %s\n", user.Name)
})
```

### `Publish(eventID E, data any)`

Publishes an event asynchronously.

```go
bus.Publish(EventUserCreated, User{
	ID:   1,
	Name: "John",
})
```

### `SetPanicHandler(handler func(any))`

Sets a custom panic handler used to recover from panics occurring inside event handlers.

```go
bus.SetPanicHandler(func(r any) {
	log.Printf("Recovered panic: %v\n", r)
})
```

### `Close()`

Stops the event bus and waits for all queued events to be processed.

```go
defer bus.Close()
```

## Examples

### String-Based Event IDs

```go
type EventID string

const (
	EventGreeting EventID = "greeting"
	EventCounter  EventID = "counter"
)

bus := eventbus.New[EventID](2, 10)
defer bus.Close()

bus.Register(EventGreeting, func(e eventbus.Event[EventID]) {
	fmt.Println("Message:", e.Data.(string))
})

bus.Register(EventCounter, func(e eventbus.Event[EventID]) {
	fmt.Println("Count:", e.Data.(int))
})

bus.Publish(EventGreeting, "Hello")
bus.Publish(EventCounter, 42)
```

### Enum-Style Event IDs

```go
type EventID int

const (
	EventUserCreated EventID = iota
	EventUserUpdated
	EventUserDeleted
)

bus := eventbus.New[EventID](4, 100)

bus.Register(EventUserCreated, func(e eventbus.Event[EventID]) {
	fmt.Println("User created")
})

bus.Publish(EventUserCreated, nil)
```

### Custom Payload Types

```go
type EventID string

const (
	EventUserSignup EventID = "user.signup"
)

type User struct {
	ID   int
	Name string
}

bus.Register(EventUserSignup, func(e eventbus.Event[EventID]) {
	user := e.Data.(User)

	fmt.Printf(
		"New user: %s (ID: %d)\n",
		user.Name,
		user.ID,
	)
})

bus.Publish(EventUserSignup, User{
	ID:   1,
	Name: "Alice",
})
```

### Custom Event Identifier Types

```go
type EventID struct {
	Domain string
	Action string
}

bus := eventbus.New[EventID](2, 10)

bus.Register(EventID{
	Domain: "user",
	Action: "created",
}, func(e eventbus.Event[EventID]) {
	fmt.Println("User created")
})

bus.Publish(EventID{
	Domain: "user",
	Action: "created",
}, nil)
```

Note: All fields inside the struct must be comparable.

### Type Switch Pattern

```go
bus.Register(EventUniversal, func(e eventbus.Event[EventID]) {
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

### Panic Recovery

```go
bus.SetPanicHandler(func(r any) {
	log.Printf("Panic in handler: %v\n", r)
})

bus.Register(EventRisky, func(e eventbus.Event[EventID]) {
	panic("something went wrong")
})
```

## How It Works

1. Event handlers are registered for specific event identifiers.
2. Events are published to an internal buffered queue.
3. Worker goroutines consume events concurrently.
4. Registered handlers are executed sequentially for each event.
5. Panics are recovered and forwarded to the configured panic handler.

## Thread Safety

The event bus is fully thread-safe:

* Event registration is protected by `sync.RWMutex`
* Event publishing is safe from multiple goroutines
* Worker goroutines process events concurrently
* Generic event identifiers (`E comparable`) are safely handled
* All public methods may be called concurrently

## Testing

Run the test suite:

```bash
go test -v
```

Run the race detector:

```bash
go test -race
```

## Configuration Tips

### Worker Count

* More workers are usually beneficial for I/O-bound handlers
* Fewer workers may be preferable for CPU-heavy workloads
* A good starting point is `runtime.NumCPU()`

### Queue Size

* Larger queue sizes absorb traffic bursts better
* Smaller queue sizes reduce memory usage
* Choose a size based on expected event throughput

## Contributing

Contributions are welcome.

Feel free to open issues, submit pull requests, or suggest improvements.

## License

This project is licensed under the GNU General Public License v3.0 (GPLv3).

See the LICENSE file for details.

## Author

[dokuzsertkol](https://github.com/dokuzsertkol)
