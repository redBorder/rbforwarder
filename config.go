package rbforwarder

// Config stores the configuration for a forwarder
type Config struct {
	Retries   int
	Backoff   int
	QueueSize int
}
