package types

// Reporter returns information about a processed message. The GetOpts method
// should return, at least, the same info that was pushed to the original
// message from the user.
type Reporter interface {
	Status() (code int, status string, retries int)
	GetOpts() map[string]interface{}
}
