package main

import (
	"fmt"

	"github.com/dokuzsertkol/eventbus-go"
)

// Custom types
type User struct {
	ID   int
	Name string
}

type Product struct {
	ID    string
	Price float64
}

func main() {
	// create bus with 2 workers and 10 queue size
	bus := eventbus.New(2, 10)
	defer bus.Close()

	// set panic handler
	bus.SetPanicHandler(func(r any) {
		fmt.Printf("Panic caught: %v\n", r)
	})

	// string handler
	bus.Register("greeting", func(e eventbus.Event) {
		msg := e.Data.(string)
		fmt.Printf("Message: %s\n", msg)
	})

	// int handler with safe assertion
	bus.Register("double", func(e eventbus.Event) {
		if num, ok := e.Data.(int); ok {
			fmt.Printf("%d * 9 = %d\n", num, num*9)
		} else {
			fmt.Printf("Expected int, got %T\n", e.Data)
		}
	})

	// custom struct handler
	bus.Register("user.created", func(e eventbus.Event) {
		user := e.Data.(User)
		fmt.Printf("User created: %d - %s\n", user.ID, user.Name)
	})

	// multiple types in one handler (type switch)
	bus.Register("printer", func(e eventbus.Event) {
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

	// string
	bus.Publish("greeting", "Feel free to contribute!")

	// integer
	bus.Publish("double", 9)
	bus.Publish("double", "not a number") // will trigger panic handler

	// custom struct
	bus.Publish("user.created", User{ID: 1, Name: "DokuzSertkol"})

	// type switch examples
	bus.Publish("printer", "string value")
	bus.Publish("printer", 999)
	bus.Publish("printer", User{ID: 9, Name: "DokuzSertkol"})
	bus.Publish("printer", Product{ID: "9", Price: 99.99})

	// wrong type (panic handler will catch)
	bus.Publish("user.created", "invalid user") // string instead of User
}
