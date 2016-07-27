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
func (m message) GetOpt(name string) (opt interface{}, err error) {
	if opt = m.opts[name]; opt == nil {
		err = errors.New("No option available: " + name)
	}

	return
}

func (m message) GetReport() Report {
	return Report{
		code:    m.code,
		status:  m.status,
		retries: m.retries,
		opts:    m.opts,
	}
}

// Done send the report to the report handler so it can be delivered to the
// user
func (m *message) Done(code int, status string) {
	m.code = code
	m.status = status
	m.channel <- m
}
