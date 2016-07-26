package rbforwarder

// Config stores the configuration for a forwarder
type Config struct {
	Retries     int
	Backoff     int
	Workers     int
	QueueSize   int
	MaxMessages int
	MaxBytes    int
	ShowCounter int
}
