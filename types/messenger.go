package types

// Messenger is used by modules to handle messages
type Messenger interface {
	PopData() ([]byte, error)
	PopOpts() (map[string]interface{}, error)
	Reports() []Reporter
}
