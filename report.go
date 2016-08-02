package rbforwarder

// Report contains information about a produced message
type Report struct {
	code    int
	status  string
	retries int
	opts    map[string]interface{}
}
