package util

import (
	"github.com/Sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var Level = logrus.InfoLevel

func NewLogger(prefix string) *logrus.Entry {
	log := logrus.New()
	log.Formatter = new(prefixed.TextFormatter)
	log.Level = Level
	logger := log.WithFields(logrus.Fields{
		"prefix": prefix,
	})

	return logger
}
