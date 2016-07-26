package pipeline

// Sender takes a raw buffer and sent it using a network protocol
type Sender interface {
	Init(int, chan *Message) error
	OnMessage(*Message) error
}
