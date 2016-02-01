package senders

import (
	"github.com/Bigomby/go-pipes/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

type Sender interface {
	Init(int) error
	Send(*util.Message) error
}

func NewSender(config util.ElementConfig) Sender {
	log = util.NewLogger("sender")

	switch config.Type {
	case "stdout":
	default:
		return &StdoutSender{
			rawConfig: config.Config,
		}
		break
	}

	return nil
}
