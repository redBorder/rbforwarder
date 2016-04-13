package httpsender

import (
	"log"
	"time"

	"github.com/redBorder/rbforwarder"
)

// Helper is used to create instances of HTTP senders
type Helper struct {
	config config
}

// NewHelper creates a new sender helper
func NewHelper(rawConfig map[string]interface{}) Helper {
	logger = rbforwarder.NewLogger("sender")
	parsedConfig, _ := parseConfig(rawConfig)

	return Helper{
		config: parsedConfig,
	}
}

// CreateSender returns an instance of HTTP Sender
func (s Helper) CreateSender() rbforwarder.Sender {
	httpSender := new(Sender)
	httpSender.config = s.config

	return httpSender
}

// Parse the config from YAML file
func parseConfig(raw map[string]interface{}) (parsed config, err error) {
	if raw["url"] != nil {
		parsed.URL = raw["url"].(string)
	} else {
		log.Fatal("No url provided")
	}

	if raw["endpoint"] != nil {
		parsed.Endpoint = raw["endpoint"].(string)
	}

	if raw["insecure"] != nil {
		parsed.IgnoreCert = raw["insecure"].(bool)
		if parsed.IgnoreCert {
			logger.Warn("Ignoring SSL certificates")
		}
	}

	if raw["batchsize"] != nil {
		parsed.BatchSize = int64(raw["batchsize"].(int))
	} else {
		parsed.BatchSize = 1
	}

	if raw["batchtimeout"] != nil {
		parsed.BatchTimeout = time.Duration(raw["batchtimeout"].(int)) * time.Millisecond
	}

	if raw["deflate"] != nil {
		parsed.Deflate = raw["deflate"].(bool)
	}

	if raw["showcounter"] != nil {
		parsed.ShowCounter = raw["showcounter"].(int)
	}

	return
}
