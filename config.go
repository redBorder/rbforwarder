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

// messageHandlerConfig is used to store the configuration for the reportHandler
type messageHandlerConfig struct {
	maxRetries int
	backoff    int
	queueSize  int
}
