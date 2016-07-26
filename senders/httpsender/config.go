package httpsender

import "time"

type config struct {
	URL          string
	Endpoint     string
	IgnoreCert   bool
	Deflate      bool
	ShowCounter  int
	BatchSize    int64
	BatchTimeout time.Duration
}
