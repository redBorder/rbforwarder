package processors

import (
	"github.com/Bigomby/go-pipes/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

type Processor interface {
	Init(int) error
	Process(message *util.Message) (bool, error)
	Alert() chan bool
	AlertHandler() (*util.Message, error)
}

func NewProcessor(config util.ElementConfig) Processor {
	log = util.NewLogger("processor")

	switch config.Type {
	default:
		return &NullProcessor{
			rawConfig: config.Config,
		}
		break
	}

	return nil
}
