package eventbus

type Handler[E comparable] func(Event[E])
