package rbforwarder

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/redBorder/rbforwarder/components/batch"
	"github.com/redBorder/rbforwarder/components/httpsender"
)

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

func BenchmarkNoBatch(b *testing.B) {
	var components []interface{}
	var workers []int

	f := NewRBForwarder(Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: 1,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			TimeoutMillis: 1000,
			Limit:         10000,
		},
	}
	components = append(components, batch)
	workers = append(workers, 1)

	sender := &httpsender.HTTPSender{
		URL:    "http://localhost:8888",
		Client: NewTestClient(200, func(r *http.Request) {}),
	}
	components = append(components, sender)
	workers = append(workers, 1)

	f.PushComponents(components, workers)
	f.Run()

	opts := map[string]interface{}{
		"http_endpoint": "librb-http",
		"batch_group":   "librb-http",
	}

	for i := 0; i < b.N; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	for report := range f.GetReports() {
		r := report.(Report)
		if r.Code > 0 {
			b.FailNow()
		}
		if r.Opaque.(int) == b.N-1 {
			break
		}
	}
}

func BenchmarkLittleBatch(b *testing.B) {
	var components []interface{}
	var workers []int

	f := NewRBForwarder(Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: b.N / 100,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			TimeoutMillis: 1000,
			Limit:         10000,
		},
	}
	components = append(components, batch)
	workers = append(workers, 1)

	sender := &httpsender.HTTPSender{
		URL:    "http://localhost:8888",
		Client: NewTestClient(200, func(r *http.Request) {}),
	}
	components = append(components, sender)
	workers = append(workers, 1)

	f.PushComponents(components, workers)
	f.Run()

	opts := map[string]interface{}{
		"http_endpoint": "librb-http",
		"batch_group":   "librb-http",
	}

	for i := 0; i < b.N; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	for report := range f.GetReports() {
		r := report.(Report)
		if r.Code > 0 {
			b.FailNow()
		}
		if r.Opaque.(int) == b.N-1 {
			break
		}
	}
}

func BenchmarkBigBatch(b *testing.B) {
	var components []interface{}
	var workers []int

	f := NewRBForwarder(Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: b.N / 10,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			TimeoutMillis: 1000,
			Limit:         10000,
		},
	}
	components = append(components, batch)
	workers = append(workers, 1)

	sender := &httpsender.HTTPSender{
		URL:    "http://localhost:8888",
		Client: NewTestClient(200, func(r *http.Request) {}),
	}
	components = append(components, sender)
	workers = append(workers, 1)

	f.PushComponents(components, workers)
	f.Run()

	opts := map[string]interface{}{
		"http_endpoint": "librb-http",
		"batch_group":   "librb-http",
	}

	for i := 0; i < b.N; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	for report := range f.GetReports() {
		r := report.(Report)
		if r.Code > 0 {
			b.FailNow()
		}
		if r.Opaque.(int) == b.N-1 {
			break
		}
	}
}
