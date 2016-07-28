package httpsender

import "time"

// Config contains the configuration for an HTTP Sender
type Config struct {
	URL          string
	Endpoint     string
	IgnoreCert   bool
	Deflate      bool
	ShowCounter  int
	BatchSize    int64
	BatchTimeout time.Duration
}
