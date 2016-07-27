package pipeline

// Sender takes a raw buffer and sent it using a network protocol
type Sender interface {
	Init(id int) error
	OnMessage(Messenger)
}
