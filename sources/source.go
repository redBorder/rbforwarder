package sources

import (
	"github.com/Sirupsen/logrus"
	"github.com/redBorder/rbforwarder"
)

var log *logrus.Entry

// NewListener creates a new listener depending on the configuration passed
// as argument
func NewListener(config rbforwarder.ListenerConfig) (listener rbforwarder.Listener) {
	switch config.Type {
	default:
	}

	return
}
