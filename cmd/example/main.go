package main

import (
	"fmt"

	"github.com/dokuzsertkol/eventbus-go"
)

type EventType int

const (
	EventGreeting EventType = iota
	EventDouble
	EventUserCreated
	EventPrinter
)

type User struct {
	ID   int
	Name string
}

type Product struct {
	ID    string
	Price float64
}

func main() {
	bus := eventbus.New[EventType](2, 10)
	defer bus.Close()

	bus.SetPanicHandler(func(r any) {
		fmt.Printf("Panic caught: %v\n", r)
	})

	bus.Register(EventGreeting, func(e eventbus.Event[EventType]) {
		msg := e.Data.(string)
		fmt.Printf("Message: %s\n", msg)
	})

	bus.Register(EventDouble, func(e eventbus.Event[EventType]) {
		if num, ok := e.Data.(int); ok {
			fmt.Printf("%d * 9 = %d\n", num, num*9)
		} else {
			fmt.Printf("Expected int, got %T\n", e.Data)
		}
	})

	bus.Register(EventUserCreated, func(e eventbus.Event[EventType]) {
		user := e.Data.(User)
		fmt.Printf("User created: %d - %s\n", user.ID, user.Name)
	})

	bus.Register(EventPrinter, func(e eventbus.Event[EventType]) {
		switch v := e.Data.(type) {
		case string:
			fmt.Printf("String: %s\n", v)
		case int:
			fmt.Printf("Integer: %d\n", v)
		case User:
			fmt.Printf("User: %s (ID: %d)\n", v.Name, v.ID)
		case Product:
			fmt.Printf("Product: %s - $%.2f\n", v.ID, v.Price)
		default:
			fmt.Printf("Unknown type: %T\n", v)
		}
	})

	bus.Publish(EventGreeting, "Feel free to contribute!")
	bus.Publish(EventDouble, 9)
	bus.Publish(EventDouble, "not a number")
	bus.Publish(EventUserCreated, User{ID: 1, Name: "DokuzSertkol"})
	bus.Publish(EventPrinter, "string value")
	bus.Publish(EventPrinter, 999)
	bus.Publish(EventPrinter, User{ID: 9, Name: "DokuzSertkol"})
	bus.Publish(EventPrinter, Product{ID: "9", Price: 99.99})
	bus.Publish(EventUserCreated, "invalid user")

	fmt.Println("Press Enter to exit...")
	fmt.Scanln()
}
