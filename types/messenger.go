package types

// Messenger is used by modules to handle messages
type Messenger interface {
	PopData() ([]byte, error)
	PushData(data []byte)
	GetOpt(name string) (interface{}, error)
}
