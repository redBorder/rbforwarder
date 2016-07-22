package rbforwarder

import "bytes"

// Message is used to send data through the pipeline
type Message struct {
	InputBuffer  *bytes.Buffer          // The original data from the source
	Data         interface{}            // Can be used to store the data once it has been parsed
	OutputBuffer *bytes.Buffer          // The data that will be sent by the sender
	Metadata     map[string]interface{} // Opaque

	Report Report
}
