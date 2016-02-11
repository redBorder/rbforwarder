package processors

import (
	"github.com/redBorder/rb-forwarder/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

// Processor is an interface for object that process a data structure and
// performs chantes on it
type Processor interface {
	Init(int) error
	Process(message *util.Message) (bool, error)
	Alert() chan bool
	AlertHandler() (*util.Message, error)
}

// NewProcessor is a helper for create a new processor object
func NewProcessor(config util.ElementConfig) Processor {
	log = util.NewLogger("processor")

	switch config.Type {
	default:
		return &NullProcessor{
			rawConfig: config.Config,
		}
	}
}
