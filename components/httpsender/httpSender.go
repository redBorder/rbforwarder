// Copyright (C) ENEO Tecnologia SL - 2016
//
// Authors: Diego Fernández Barrera <dfernandez@redborder.com> <bigomby@gmail.com>
// 					Eugenio Pérez Martín <eugenio@redborder.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/lgpl-3.0.txt>.

package httpsender

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/asaskevich/govalidator"
	"github.com/redBorder/rbforwarder/utils"
)

// HTTPSender is a component for the rbforwarder pipeline that sends messages
// to an HTTP endpoint. It's a final component, so it will call Done() instead
// of Next() and further components shuld not be added after this component.
type HTTPSender struct {
	id     int
	err    error
	Client *http.Client
	logger *logrus.Entry

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

	if httpsender.Config.Logger == nil {
		s.logger = logrus.NewEntry(logrus.New())
		s.logger.Logger.Out = ioutil.Discard
	} else {
		s.logger = httpsender.Config.Logger.WithFields(logrus.Fields{
			"worker": id,
		})
	}

	if httpsender.Debug {
		s.logger.Logger.Level = logrus.DebugLevel
	}

	s.logger.Debugf("Spawning worker")

	if govalidator.IsURL(s.URL) {
		s.Client = new(http.Client)

		if httpsender.Config.Insecure {
			s.Client.Transport = &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
		}
	} else {
		s.err = errors.New("Invalid URL")
	}

	return &s
}

// OnMessage is called when a new message should be sent via HTTP
func (httpsender *HTTPSender) OnMessage(m *utils.Message, done utils.Done) {
	var u string
	var headers map[string]string
	var code int
	var status string

	if httpsender.err != nil {
		httpsender.logger.Debugf("Could not send message: %v", httpsender.err)
		done(m, 100, httpsender.err.Error())
		return
	}

	data, err := m.PopPayload()
	defer func() {
		m.PushPayload(data)
		done(m, code, status)
	}()

	if err != nil {
		httpsender.logger.Debugf("Could not send message: %v", err)
		code = 101
		status = "Can't get payload of message: " + err.Error()
		return
	}

	if endpoint, exists := m.Opts.Get("http_endpoint"); exists {
		u = httpsender.URL + "/" + endpoint.(string)
	} else {
		u = httpsender.URL
	}

	buf := bytes.NewBuffer(data)
	req, _ := http.NewRequest("POST", u, buf)

	if h, exists := m.Opts.Get("http_headers"); exists {
		headers = h.(map[string]string)
		for k, v := range headers {
			req.Header.Add(k, v)
		}
	}

	res, err := httpsender.Client.Do(req)
	if err != nil {
		httpsender.logger.Warnf(err.Error())
		code = 1
		status = "HTTPSender error: " + err.Error()
		return
	}
	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()

	if res.StatusCode >= 400 {
		httpsender.logger.Warnf("Got status: %v", res.Status)
		code = 2
		status = "HTTPSender error: " + res.Status
		return
	}

	code = 0
	status = res.Status
	httpsender.logger.Debugf("Sent message: %v", string(buf.Bytes()))
}
