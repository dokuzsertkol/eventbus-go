package eventbus

type Event[E comparable] struct {
	ID   E
	Data any
}
