package rbforwarder

import (
	"errors"

	"github.com/oleiade/lane"
)

// message is used to send data through the pipeline
type message struct {
	payload *lane.Stack
	opts    *lane.Stack
	seq     uint64 // Unique ID for the report, used to maintain sequence
	status  string // Result of the sending
	code    int    // Result of the sending
	retries int
}

// PushData store data on an LIFO queue so the nexts handlers can use it
func (m *message) PushData(v []byte) {
	if m.payload == nil {
		m.payload = lane.NewStack()
	}

	m.payload.Push(v)
}

// PopData get the data stored by the previous handler
func (m *message) PopData() (ret []byte, err error) {
	if m.payload.Empty() {
		err = errors.New("Empty stack")
		return
	}
	ret = m.payload.Pop().([]byte)

	return
}

func (m message) GetReports() []Report {
	var reports []Report

	for !m.opts.Empty() {
		reports = append(reports, Report{
			code:    m.code,
			status:  m.status,
			retries: m.retries,
			opts:    m.opts.Pop().(map[string]interface{}),
		})
	}

	return reports
}