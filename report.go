package rbforwarder

// Report contains information abot a delivered message
type Report struct {
	Code   int
	Status string
	Opaque interface{}

	seq     uint64
	retries int
}
