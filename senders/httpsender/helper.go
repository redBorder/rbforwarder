package httpsender

import (
	"log"
	"time"

	"github.com/redBorder/rbforwarder"
)

// Helper is used to create instances of HTTP senders
type Helper struct {
	rawConfig map[string]interface{}
}

// NewHelper creates a new sender helper
func NewHelper(rawConfig map[string]interface{}) Helper {
	return Helper{
		rawConfig: rawConfig,
	}
}

// CreateSender returns an instance of HTTP Sender
func (s Helper) CreateSender() rbforwarder.Sender {
	httpSender := new(Sender)
	httpSender.config, _ = parseConfig(s.rawConfig)

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
