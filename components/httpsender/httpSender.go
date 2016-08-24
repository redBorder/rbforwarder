package httpsender

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/asaskevich/govalidator"
	"github.com/redBorder/rbforwarder/utils"
)

// HTTPSender is a component for the rbforwarder pipeline that sends messages
// to an HTTP endpoint. It's a final component, so it will call Done() instead
// of Next() and further components shuld not be added after this component.
type HTTPSender struct {
	id  int
	err error

	Config
}

// Workers returns the number of workers
func (httpsender *HTTPSender) Workers() int {
	return httpsender.Config.Workers
}

// Spawn initializes the HTTP component
func (httpsender *HTTPSender) Spawn(id int) utils.Composer {
	s := *httpsender

	s.id = id

	if govalidator.IsURL(s.URL) {
		if s.Client == nil {
			s.Client = &http.Client{}
		}
	} else {
		s.err = errors.New("Invalid URL")
	}

	return &s
}

// OnMessage is called when a new message should be sent via HTTP
func (httpsender *HTTPSender) OnMessage(m *utils.Message, done utils.Done) {
	var u string

	if httpsender.err != nil {
		done(m, 2, httpsender.err.Error())
		return
	}

	data, err := m.PopPayload()
	if err != nil {
		done(m, 3, "Can't get payload of message: "+err.Error())
		return
	}

	if endpoint, exists := m.Opts.Get("http_endpoint"); exists {
		u = httpsender.URL + "/" + endpoint.(string)
	} else {
		u = httpsender.URL
	}

	buf := bytes.NewBuffer(data)
	res, err := httpsender.Client.Post(u, "", buf)
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
