package pipeline

// Messenger is used by modules to handle messages
type Messenger interface {
	PopData() ([]byte, error)
	PushData(data []byte)
	Done(code int, status string)
	GetOpt(name string) (interface{}, error)
}
