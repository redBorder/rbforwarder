package httpsender

// Config exposes the configuration for an HTTP Sender
type Config struct {
	Workers  int
	URL      string
	Insecure bool
}
