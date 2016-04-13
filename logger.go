package rbforwarder

import (
	"github.com/Sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var level = logrus.InfoLevel

// LogLevel sets logging level
func LogLevel(newLevel logrus.Level) {
	level = newLevel
}

// NewLogger creates a new logger object
func NewLogger(prefix string) *logrus.Entry {
	log := logrus.New()
	log.Formatter = new(prefixed.TextFormatter)
	log.Level = level

	logger := log.WithFields(logrus.Fields{
		"prefix": prefix,
	})

	return logger
}
