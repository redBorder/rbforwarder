package rbforwarder

import (
	"errors"

	"github.com/oleiade/lane"
	"github.com/redBorder/rbforwarder/types"
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
	if m.payload == nil {
		err = errors.New("Uninitialized payload")
		return
	}

	if m.payload.Empty() {
		err = errors.New("No data")
		return
	}

	ret = m.payload.Pop().([]byte)
	return
}

// PopData get the data stored by the previous handler
func (m *message) PopOpts() (ret map[string]interface{}, err error) {
	if m.opts == nil {
		err = errors.New("Uninitialized options")
		return
	}

	if m.opts.Empty() {
		err = errors.New("No options")
		return
	}

	ret = m.opts.Pop().(map[string]interface{})
	return
}

func (m message) Reports() []types.Reporter {
	var reports []types.Reporter

	for !m.opts.Empty() {
		reports = append(reports, report{
			code:    m.code,
			status:  m.status,
			retries: m.retries,
			opts:    m.opts.Pop().(map[string]interface{}),
		})
	}

	return reports
}
