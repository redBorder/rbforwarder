package processors

import (
	"github.com/redBorder/rb-forwarder/util"
)

type NullProcessor struct {
	id        int
	alert     chan bool
	rawConfig util.Config
}

// Creates a decoder for JSON
func (p NullProcessor) Init(i int) error {
	log.Debugf("[%d] Started Null Processor", p.id)
	return nil
}

// Process message data
func (p NullProcessor) Process(message *util.Message) (next bool, err error) {
	return true, nil
}

func (p NullProcessor) Alert() chan bool {
	return p.alert
}

func (p NullProcessor) AlertHandler() (message *util.Message, err error) {
	return
}
