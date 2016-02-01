package decoders

import (
	"github.com/Bigomby/go-pipes/util"

	"github.com/Sirupsen/logrus"
)

type Decoder interface {
	Init(int) error
	Decode(*util.Message) error
}

var log *logrus.Entry

func NewDecoder(config util.ElementConfig) Decoder {
	log = util.NewLogger("decoder")

	switch config.Type {
	default:
		return &NullDecoder{
			configRaw: config,
		}
		break
	}

	return nil
}
