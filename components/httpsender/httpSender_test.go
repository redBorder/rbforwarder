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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/redBorder/rbforwarder/utils"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/mock"
)

type Doner struct {
	mock.Mock
	doneCalled chan struct {
		code   int
		status string
	}
}

func (d *Doner) Done(m *utils.Message, code int, status string) {
	d.Called(m, code, status)
	d.doneCalled <- struct {
		code   int
		status string
	}{
		code,
		status,
	}
}

func NewTestClient(code int, cb func(*http.Request)) *http.Client {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			cb(r)
		}))

	transport := &http.Transport{
		Proxy: func(req *http.Request) (*url.URL, error) {
			return url.Parse(server.URL)
		},
	}

	return &http.Client{Transport: transport}
}

func TestHTTPSender(t *testing.T) {
	Convey("Given an HTTP sender with defined URL", t, func() {
		sender := &HTTPSender{
			Config: Config{
				Workers:  1,
				URL:      "http://example.com",
				Insecure: true,
			},
		}

		Convey("When it is initialized", func() {
			s := sender.Spawn(0).(*HTTPSender)

			Convey("Then the config should be ok", func() {
				So(s.Client, ShouldNotBeNil)
				So(s.Workers(), ShouldEqual, 1)
			})
		})

		Convey("When a message is sent and the response code is >= 400", func() {
			var url string
			s := sender.Spawn(0).(*HTTPSender)

			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))
			s.Client = NewTestClient(401, func(req *http.Request) {
				url = req.URL.String()
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}

			d.On("Done", mock.AnythingOfType("*utils.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			s.OnMessage(m, d.Done)

			Convey("Then the reporth should contain info about the error", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "HTTPSender error: 401 Unauthorized")
				So(result.code, ShouldEqual, 2)
				So(url, ShouldEqual, "http://example.com/")

				d.AssertExpectations(t)
			})
		})

		Convey("When a message is received without endpoint option", func() {
			var url string
			s := sender.Spawn(0).(*HTTPSender)

			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))

			s.Client = NewTestClient(200, func(req *http.Request) {
				url = req.URL.String()
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*utils.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			s.OnMessage(m, d.Done)

			Convey("Then the message should be sent via HTTP to the URL", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "200 OK")
				So(result.code, ShouldBeZeroValue)
				So(url, ShouldEqual, "http://example.com/")

				d.AssertExpectations(t)
			})
		})

		Convey("When a message is received with endpoint option", func() {
			var url string
			s := sender.Spawn(0).(*HTTPSender)

			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))
			m.Opts.Set("http_endpoint", "endpoint1")

			s.Client = NewTestClient(200, func(req *http.Request) {
				url = req.URL.String()
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*utils.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			s.OnMessage(m, d.Done)

			Convey("Then the message should be sent to the URL with endpoint as suffix", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "200 OK")
				So(result.code, ShouldBeZeroValue)
				So(url, ShouldEqual, "http://example.com/endpoint1")

				d.AssertExpectations(t)
			})
		})

		Convey("When a message is received with headers", func() {
			var url string
			var headerValue string

			s := sender.Spawn(0).(*HTTPSender)
			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))
			m.Opts.Set("http_headers", map[string]string{
				"key": "value",
			})

			s.Client = NewTestClient(200, func(req *http.Request) {
				url = req.URL.String()
				headerValue = req.Header.Get("key")
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*utils.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			s.OnMessage(m, d.Done)

			Convey("Then the message should be sent with headers set", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "200 OK")
				So(result.code, ShouldBeZeroValue)
				So(url, ShouldEqual, "http://example.com/")
				So(headerValue, ShouldEqual, "value")

				d.AssertExpectations(t)
			})
		})

		Convey("When a message without payload is received", func() {
			var url string
			s := sender.Spawn(0).(*HTTPSender)

			m := utils.NewMessage()

			s.Client = NewTestClient(200, func(req *http.Request) {
				url = req.URL.String()
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*utils.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			s.OnMessage(m, d.Done)

			Convey("Then the message should not be sent", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "Can't get payload of message: No payload available")
				So(result.code, ShouldBeGreaterThan, 0)
				So(url, ShouldBeEmpty)

				d.AssertExpectations(t)
			})
		})

		Convey("When a the HTTP client fails", func() {
			s := sender.Spawn(0).(*HTTPSender)

			m := utils.NewMessage()
			m.PushPayload(nil)

			s.Client = NewTestClient(200, func(req *http.Request) {
				req.Write(nil)
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*utils.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			s.OnMessage(m, d.Done)

			Convey("Then the message should not be sent", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "HTTPSender error: Post http://example.com: EOF")
				So(result.code, ShouldBeGreaterThan, 0)

				d.AssertExpectations(t)
			})
		})
	})

	Convey("Given an HTTP sender with invalid URL", t, func() {
		sender := &HTTPSender{
			Config: Config{Insecure: true},
		}
		s := sender.Spawn(0).(*HTTPSender)

		Convey("When try to send messages", func() {
			m := utils.NewMessage()
			m.PushPayload([]byte("Hello World"))
			m.Opts.Set("http_endpoint", "endpoint1")

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}

			d.On("Done", mock.AnythingOfType("*utils.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			s.OnMessage(m, d.Done)

			Convey("Then should fail to send messages", func() {
				So(s.err, ShouldNotBeNil)
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "Invalid URL")
				So(result.code, ShouldBeGreaterThan, 0)
			})
		})
	})
}
