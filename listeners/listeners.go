package listeners

import (
	"github.com/redBorder/rb-forwarder/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

// Listener is an interface for listeners objects
type Listener interface {
	Listen(*util.MessagePool) chan *util.Message
	Close()
}

// NewListener is a helper for create a new listener object
func NewListener(config util.ElementConfig) Listener {
	log = util.NewLogger("listener")

	switch config.Type {
	case "kafka":
		return &KafkaListener{
			rawConfig: config.Config,
		}
	case "synthetic":
		return &SyntheticProducer{
			rawConfig: config.Config,
		}
	default:
		log.Fatal("No listener info on config file")
		break
	}

	return nil
}
