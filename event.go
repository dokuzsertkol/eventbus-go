package eventbus

type Event[T any] struct {
	ID   string
	Data T
}
