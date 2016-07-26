package httpsender

import (
	"net/http"
	"testing"

	"github.com/redBorder/rbforwarder/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestInitSuccess(t *testing.T) {
	reports := make(chan *pipeline.Message)
	httpSender := &Sender{
		raw: map[string]interface{}{
			"url": "http://example.com",
		},
	}
	httpSender.config.ShowCounter = 1
	httpSender.config.IgnoreCert = true

	err := httpSender.Init(0, reports)

	assert.NoError(t, err)
	assert.Equal(t, 0, httpSender.id)
	assert.NotNil(t, httpSender.batchBuffer)
	assert.True(t, httpSender.client.Transport.(*http.Transport).
		TLSClientConfig.InsecureSkipVerify)
}
