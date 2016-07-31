package rbforwarder

import "errors"

// message is used to send data through the pipeline
type message struct {
	bufferStack [][]byte

	seq     uint64 // Unique ID for the report, used to maintain sequence
	status  string // Result of the sending
	code    int    // Result of the sending
	retries int
	opts    map[string]interface{}
	channel chan *message
}

// PushData store data on an LIFO queue so the nexts handlers can use it
func (m *message) PushData(v []byte) {
	m.bufferStack = append(m.bufferStack, v)
}

// PopData get the data stored by the previous handler
func (m *message) PopData() (ret []byte, err error) {
	if len(m.bufferStack) < 1 {
		err = errors.New("No data on the stack")
		return
	}

	ret = m.bufferStack[len(m.bufferStack)-1]
	m.bufferStack = m.bufferStack[0 : len(m.bufferStack)-1]

	return
}

// GetOpt returns an option
func (m message) GetOpt(name string) interface{} {
	return m.opts[name]
}

func (m message) GetReport() Report {
	return Report{
		code:    m.code,
		status:  m.status,
		retries: m.retries,
		opts:    m.opts,
	}
}
