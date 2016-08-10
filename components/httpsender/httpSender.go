package httpsender

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/asaskevich/govalidator"
	"github.com/redBorder/rbforwarder/types"
)

// HTTPSender is a component for the rbforwarder pipeline that sends messages
// to an HTTP endpoint. It's a final component, so it will call Done() instead
// of Next() and further components shuld not be added after this component.
type HTTPSender struct {
	id     int
	err    error
	URL    string
	client *http.Client
}

// Init initializes the HTTP component
func (s *HTTPSender) Init(id int) {
	s.id = id

	if govalidator.IsURL(s.URL) {
		s.client = &http.Client{}
	} else {
		s.err = errors.New("Invalid URL")
	}
}

// OnMessage is called when a new message should be sent via HTTP
func (s *HTTPSender) OnMessage(m *types.Message, next types.Next, done types.Done) {
	var u string

	if s.err != nil {
		done(m, 2, s.err.Error())
		return
	}

	data, err := m.PopPayload()
	if err != nil {
		done(m, 3, "Can't get payload of message: "+err.Error())
		return
	}

	if endpoint, exists := m.Opts["http_endpoint"]; exists {
		u = s.URL + "/" + endpoint.(string)
	} else {
		u = s.URL
	}

	buf := bytes.NewBuffer(data)
	res, err := s.client.Post(u, "", buf)
	if err != nil {
		done(m, 1, "HTTPSender error: "+err.Error())
		return
	}
	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()

	if res.StatusCode >= 400 {
		done(m, res.StatusCode, "HTTPSender error: "+res.Status)
		return
	}

	done(m, 0, res.Status)
}
