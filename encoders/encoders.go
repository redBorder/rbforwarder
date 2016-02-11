package encoders

import (
	"github.com/redBorder/rb-forwarder/util"

	"github.com/Sirupsen/logrus"
)

var log *logrus.Entry

// Encoder is an interface for objects that encode a data structure to a buffer
type Encoder interface {
	Init(int) error
	Encode(*util.Message) error
}

// NewEncoder is a helper for create a new encoder object
func NewEncoder(config util.ElementConfig) Encoder {
	log = util.NewLogger("encoder")

	switch config.Type {
	default:
		return &NullEncoder{
			configRaw: config,
		}
	}
}
