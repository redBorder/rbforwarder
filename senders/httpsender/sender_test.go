package httpsender

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitSuccess(t *testing.T) {
	httpSender := new(Sender)
	httpSender.config.ShowCounter = 1
	httpSender.config.IgnoreCert = true

	err := httpSender.Init(0)

	assert.NoError(t, err)
	assert.Equal(t, 0, httpSender.id)
	assert.NotNil(t, httpSender.batchBuffer)
	assert.True(t, httpSender.client.Transport.(*http.Transport).
		TLSClientConfig.InsecureSkipVerify)
}
