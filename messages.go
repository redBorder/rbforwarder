package rbforwarder

import (
	"bytes"
	"errors"
	"sync/atomic"
	"time"
)

const (
	statusOk = 0
)

// Report is used by the source to obtain the status of a sent message
type Report struct {
	ID         int64  // Unique ID for the report, used to maintain sequence
	Status     string // Result of the sending
	StatusCode int    // Result of the sending
	Retries    int
	Metadata   map[string]interface{}
}

// Message is used to send data to the backend
type Message struct {
	InputBuffer  *bytes.Buffer          // The original data from the source
	Data         interface{}            // Can be used to store the data once it has been parsed
	OutputBuffer *bytes.Buffer          // The data that will be sent by the sender
	Metadata     map[string]interface{} // Opaque

	report  Report
	backend *backend // Use to send the message to the backend
}

// Produce is used by the source to send messages to the backend
func (m *Message) Produce() error {

	// This is no a retry
	if m.report.Retries == 0 {
		m.report = Report{
			ID:       atomic.AddInt64(&m.backend.currentProducedID, 1) - 1,
			Metadata: m.Metadata,
		}
	}

	select {
	case messageChannel := <-m.backend.decoderPool:
		select {
		case messageChannel <- m:
		case <-time.After(1 * time.Second):
			return errors.New("Error on produce: Full queue")
		}
	case <-time.After(1 * time.Second):
		if err := m.Report(-1, "Error on produce: No workers available"); err != nil {
			return err
		}
	}

	return nil
}

// Report is used by the sender to inform that a message has not been sent
func (m *Message) Report(statusCode int, status string) error {
	m.report.StatusCode = statusCode
	m.report.Status = status
	select {
	case m.backend.reports <- m:
	case <-time.After(1 * time.Second):
		return errors.New("Error on report: Full queue")
	}

	return nil
}
