package util

import (
	"bytes"
)

type MessagePool struct {
	messages chan *Message
}

var log = NewLogger("message-pool")

func NewMessagePool(size int) *MessagePool {

	messagePool := &MessagePool{
		messages: make(chan *Message, size),
	}

	for i := 0; i < size; i++ {
		message := &Message{
			InputBuffer:  new(bytes.Buffer),
			OutputBuffer: new(bytes.Buffer),
			Attributes:   make(map[string]string),
		}

		messagePool.messages <- message
	}

	return messagePool
}

func (m *MessagePool) Take() *Message {
	var message *Message

	select {
	case message = <-m.messages:
		// log.Printf("Got message from pool")
		break
	default:
		// log.Printf("New message!")
		message = &Message{
			InputBuffer:  new(bytes.Buffer),
			OutputBuffer: new(bytes.Buffer),
			Attributes:   make(map[string]string),
		}
		break
	}

	return message
}

// Put back the message on the pool if there is room for it
func (m *MessagePool) Give(message *Message) {

	message.InputBuffer.Reset()
	message.OutputBuffer.Reset()
	message.Attributes = make(map[string]string)

	select {
	case m.messages <- message:
		// log.Printf("Put back message into the pool")
		break
	default:
		// log.Printf("Destroyed message")
		break
	}
}
