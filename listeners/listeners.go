package listeners

import (
	"github.com/Bigomby/go-pipes/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

type Listener interface {
	Listen() chan *util.Message
	Close()
}

func NewListener(config util.ElementConfig) Listener {
	log = util.NewLogger("listener")

	switch config.Type {
	case "kafka":
		return &KafkaListener{
			rawConfig: config.Config,
		}
		break
	default:
		log.Fatal("No listener info on config file")
		break
	}

	return nil
}
