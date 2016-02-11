package decoders

import (
	"github.com/redBorder/rb-forwarder/util"

	"github.com/Sirupsen/logrus"
)

// Decoder is an interface for objects that decodes a raw message
type Decoder interface {
	Init(int) error
	Decode(*util.Message) error
}

var log *logrus.Entry

// NewDecoder helps to create a new decoder object
func NewDecoder(config util.ElementConfig) Decoder {
	log = util.NewLogger("decoder")

	switch config.Type {
	default:
		return &NullDecoder{
			configRaw: config,
		}
	}
}
