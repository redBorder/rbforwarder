package senders

import (
	"github.com/redBorder/rb-forwarder/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

// Sender is the interface for every sender
type Sender interface {
	Init(int) error
	Send(*util.Message) error
}

// NewSender is a helper for create a sender object
func NewSender(config util.ElementConfig) Sender {
	log = util.NewLogger("sender")

	switch config.Type {
	case "http":
		return &HTTPSender{
			rawConfig: config.Config,
		}
	case "stdout":
		return &StdoutSender{
			rawConfig: config.Config,
		}
	default:
		return &StdoutSender{
			rawConfig: config.Config,
		}
	}
}
