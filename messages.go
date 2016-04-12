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

	backend := m.backend

	// Wait if the limit has ben reached
	if backend.maxMessages > 0 && backend.currentMessages >= uint64(backend.maxMessages) {
		<-backend.keepSending
		atomic.StoreUint64(&backend.currentMessages, 0)
	} else if backend.maxBytes > 0 && backend.currentBytes >= uint64(backend.maxBytes) {
		<-backend.keepSending
		atomic.StoreUint64(&backend.currentBytes, 0)
	}

	// This is no a retry
	if m.report.Retries == 0 {
		m.report = Report{
			ID:       atomic.AddUint64(&m.backend.currentProducedID, 1) - 1,
			Metadata: m.Metadata,
		}
	}

	select {
	case messageChannel := <-m.backend.decoderPool:
		select {
		case messageChannel <- m:
			atomic.AddUint64(&backend.currentMessages, 1)
			atomic.AddUint64(&backend.currentBytes, uint64(m.OutputBuffer.Len()))
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
