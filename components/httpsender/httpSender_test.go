package httpsender

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/redBorder/rbforwarder/types"
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

func (d *Doner) Done(m *types.Message, code int, status string) {
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
			URL: "http://example.com",
		}

		Convey("When is initialized", func() {
			sender.Init(0)

			Convey("Then the config should be ok", func() {
				So(sender.client, ShouldNotBeNil)
			})
		})

		Convey("When a message is sent and the response code is >= 400", func() {
			var url string
			sender.Init(0)

			m := types.NewMessage()
			m.PushPayload([]byte("Hello World"))
			sender.client = NewTestClient(401, func(req *http.Request) {
				url = req.URL.String()
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}

			d.On("Done", mock.AnythingOfType("*types.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			sender.OnMessage(m, nil, d.Done)

			Convey("Then the reporth should contain info about the error", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "HTTPSender error: 401 Unauthorized")
				So(result.code, ShouldEqual, 401)
				So(url, ShouldEqual, "http://example.com/")

				d.AssertExpectations(t)
			})
		})

		Convey("When a message is received without endpoint option", func() {
			var url string
			sender.Init(0)

			m := types.NewMessage()
			m.PushPayload([]byte("Hello World"))

			sender.client = NewTestClient(200, func(req *http.Request) {
				url = req.URL.String()
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*types.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			sender.OnMessage(m, nil, d.Done)

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
			sender.Init(0)

			m := types.NewMessage()
			m.PushPayload([]byte("Hello World"))
			m.Opts["http_endpoint"] = "endpoint1"

			sender.client = NewTestClient(200, func(req *http.Request) {
				url = req.URL.String()
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*types.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			sender.OnMessage(m, nil, d.Done)

			Convey("Then the message should be sent to the URL with endpoint as suffix", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "200 OK")
				So(result.code, ShouldBeZeroValue)
				So(url, ShouldEqual, "http://example.com/endpoint1")

				d.AssertExpectations(t)
			})
		})

		Convey("When a message without payload is received", func() {
			var url string
			sender.Init(0)

			m := types.NewMessage()

			sender.client = NewTestClient(200, func(req *http.Request) {
				url = req.URL.String()
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*types.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			sender.OnMessage(m, nil, d.Done)

			Convey("Then the message should not be sent", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "Can't get payload of message: No payload available")
				So(result.code, ShouldBeGreaterThan, 0)
				So(url, ShouldBeEmpty)

				d.AssertExpectations(t)
			})
		})

		Convey("When a the HTTP client fails", func() {
			sender.Init(0)

			m := types.NewMessage()
			m.PushPayload([]byte("Hello World"))

			sender.client = NewTestClient(200, func(req *http.Request) {
				req.Write(nil)
			})

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}
			d.On("Done", mock.AnythingOfType("*types.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			sender.OnMessage(m, nil, d.Done)

			Convey("Then the message should not be sent", func() {
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "HTTPSender error: Post http://example.com: EOF")
				So(result.code, ShouldBeGreaterThan, 0)

				d.AssertExpectations(t)
			})
		})
	})

	Convey("Given an HTTP sender with invalid URL", t, func() {
		sender := &HTTPSender{}
		sender.Init(0)

		Convey("When try to send messages", func() {
			m := types.NewMessage()
			m.PushPayload([]byte("Hello World"))
			m.Opts["http_endpoint"] = "endpoint1"

			d := &Doner{
				doneCalled: make(chan struct {
					code   int
					status string
				}, 1),
			}

			d.On("Done", mock.AnythingOfType("*types.Message"),
				mock.AnythingOfType("int"), mock.AnythingOfType("string"))

			sender.OnMessage(m, nil, d.Done)

			Convey("Then should fail to send messages", func() {
				So(sender.err, ShouldNotBeNil)
				result := <-d.doneCalled
				So(result.status, ShouldEqual, "Invalid URL")
				So(result.code, ShouldBeGreaterThan, 0)
			})
		})
	})
}
