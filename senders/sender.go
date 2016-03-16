package senders

import (
	"github.com/Sirupsen/logrus"
	"github.com/redBorder/rbforwarder"
)

var log *logrus.Entry

// NewSender creates a new sender depending on the configuration passed as
// argument
func NewSender(config rbforwarder.SenderConfig) (sender rbforwarder.Sender) {
	switch config.Type {
	case "http":
		sender = &HTTPSender{
			rawConfig: config.Config,
		}
	default:
		sender = &StdoutSender{
			rawConfig: config.Config,
		}
	}

	return
}
