package types

import (
	"errors"

	"github.com/oleiade/lane"
)

// Message is used to send data through the pipeline
type Message struct {
	Opts    map[string]interface{}
	Reports *lane.Stack

	payload *lane.Stack
}

// NewMessage creates a new instance of Message
func NewMessage() *Message {
	return &Message{
		payload: lane.NewStack(),
		Reports: lane.NewStack(),
		Opts:    make(map[string]interface{}),
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
