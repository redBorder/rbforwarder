package utils

import (
	"errors"

	"github.com/oleiade/lane"
	"github.com/streamrail/concurrent-map"
)

// Message is used to send data through the pipeline
type Message struct {
	Opts    cmap.ConcurrentMap
	Reports *lane.Stack

	payload *lane.Stack
}

// NewMessage creates a new instance of Message
func NewMessage() *Message {
	return &Message{
		payload: lane.NewStack(),
		Reports: lane.NewStack(),
		Opts:    cmap.New(),
	}
}

// PushPayload store data on an LIFO queue so the nexts handlers can use it
func (m *Message) PushPayload(data []byte) {
	m.payload.Push(data)
}

// PopPayload get the data stored by the previous handler
func (m *Message) PopPayload() (data []byte, err error) {
	if m.payload.Empty() {
		err = errors.New("No payload available")
		return
	}

	data = m.payload.Pop().([]byte)

	return
}
