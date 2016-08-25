package httpsender

import "github.com/Sirupsen/logrus"

// Config exposes the configuration for an HTTP Sender
type Config struct {
	Workers  int
	Logger   *logrus.Entry
	Debug    bool
	URL      string
	Insecure bool
}
