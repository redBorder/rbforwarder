package httpsender

import "net/http"

// Config exposes the configuration for an HTTP Sender
type Config struct {
	Workers int
	URL     string
	Client  *http.Client
}
