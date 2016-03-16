package rbforwarder

import (
	"bytes"
	"errors"
	"time"
)

const (
	statusOk = 0
)

// Message is used to send data to the backend
type Message struct {
	InputBuffer  *bytes.Buffer          // The original data from the source
	Data         interface{}            // Can be used to store the data once it has been parsed
	OutputBuffer *bytes.Buffer          // The data that will be sent by the sender
	Metadata     map[string]interface{} // Opaque

	retries int      // Number of retries of this message
	backend *backend // Use to send the message to the backend
}

// Produce is used by the source to send messages to the backend
func (m *Message) Produce() error {
	select {
	case messageChannel := <-m.backend.decoderPool:
		select {
		case messageChannel <- m:
		case <-time.After(1 * time.Second):
			return errors.New("Error on produce: Full queue")
		}
	case <-time.After(1 * time.Second):
		return errors.New("Error on produce: No workers available")
	}

	return nil
}

// Ack is used by the sender acknowledge a message
func (m *Message) Ack(status string) error {
	report := Report{
		ID:         m.backend.currentProducedID,
		StatusCode: statusOk,
		Status:     status,
		Metadata:   m.Metadata,

		message: m,
	}

	select {
	case m.backend.reports <- report:
		m.backend.currentProducedID++
	case <-time.After(1 * time.Second):
		return errors.New("Error on report: Full queue")
	}

	return nil
}

// Fail is used by the sender to inform that a message has not been sent
func (m *Message) Fail(statusCode int, status string) error {

	// If statusCode is zero, it's actually an Ack
	if statusCode == 0 {
		return m.Ack(status)
	}

	report := Report{
		ID:         m.backend.currentProducedID,
		StatusCode: statusCode,
		Status:     status,
		Metadata:   m.Metadata,

		message: m,
	}

	select {
	case m.backend.reports <- report:
		m.backend.currentProducedID++
	case <-time.After(1 * time.Second):
		return errors.New("Error on report: Full queue")
	}

	return nil
}

// Report is used by the source to obtain the status of a sent message
type Report struct {
	ID         int64                  // Unique ID for the report, used to maintain sequence
	Status     string                 // Result of the sending
	StatusCode int                    // Result of the sending
	Metadata   map[string]interface{} // Opaque

	message *Message
}
