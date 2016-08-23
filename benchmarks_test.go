package rbforwarder

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/redBorder/rbforwarder/components/batch"
	"github.com/redBorder/rbforwarder/components/httpsender"
	"github.com/redBorder/rbforwarder/utils"
)

type Null struct{}

func (null *Null) Spawn(id int) utils.Composer {
	n := *null
	return &n
}

func (null *Null) OnMessage(m *utils.Message, done utils.Done) {
	done(m, 0, "")
}

func BenchmarkQueue1(b *testing.B)      { benchmarkQueue(1, b) }
func BenchmarkQueue10(b *testing.B)     { benchmarkQueue(10, b) }
func BenchmarkQueue1000(b *testing.B)   { benchmarkQueue(100, b) }
func BenchmarkQueue10000(b *testing.B)  { benchmarkQueue(1000, b) }
func BenchmarkQueue100000(b *testing.B) { benchmarkQueue(10000, b) }

func BenchmarkHTTP(b *testing.B) { benchmarkHTTP(b) }

func BenchmarkBatch1(b *testing.B)      { benchmarkBatch(1, b) }
func BenchmarkBatch10(b *testing.B)     { benchmarkBatch(10, b) }
func BenchmarkBatch1000(b *testing.B)   { benchmarkBatch(100, b) }
func BenchmarkBatch10000(b *testing.B)  { benchmarkBatch(1000, b) }
func BenchmarkBatch100000(b *testing.B) { benchmarkBatch(10000, b) }

func BenchmarkHTTPBatch1(b *testing.B)      { benchmarkHTTPBatch(1, b) }
func BenchmarkHTTPBatch10(b *testing.B)     { benchmarkHTTPBatch(10, b) }
func BenchmarkHTTPBatch1000(b *testing.B)   { benchmarkHTTPBatch(100, b) }
func BenchmarkHTTPBatch10000(b *testing.B)  { benchmarkHTTPBatch(1000, b) }
func BenchmarkHTTPBatch100000(b *testing.B) { benchmarkHTTPBatch(10000, b) }

func benchmarkQueue(queue int, b *testing.B) {
	var wg sync.WaitGroup
	var components []interface{}
	var workers []int

	f := NewRBForwarder(Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: queue,
	})

	null := &Null{}
	components = append(components, null)
	workers = append(workers, 1)

	f.PushComponents(components, workers)
	f.Run()

	opts := map[string]interface{}{}

	wg.Add(1)
	b.ResetTimer()
	go func() {
		for report := range f.GetReports() {
			r := report.(Report)
			if r.Code > 0 {
				b.FailNow()
			}
			if r.Opaque.(int) >= b.N-1 {
				break
			}
		}

		wg.Done()
	}()

	for i := 0; i < b.N; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	wg.Wait()
}

func benchmarkBatch(batchSize int, b *testing.B) {
	var wg sync.WaitGroup
	var components []interface{}
	var workers []int

	f := NewRBForwarder(Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: 10000,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			TimeoutMillis: 1000,
			Limit:         uint64(batchSize),
		},
	}
	components = append(components, batch)
	workers = append(workers, 1)

	f.PushComponents(components, workers)
	f.Run()

	opts := map[string]interface{}{
		"batch_group": "test",
	}

	wg.Add(1)
	b.ResetTimer()
	go func() {
		for report := range f.GetReports() {
			r := report.(Report)
			if r.Code > 0 {
				b.FailNow()
			}
			if r.Opaque.(int) >= b.N-1 {
				break
			}
		}

		wg.Done()
	}()

	for i := 0; i < b.N; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	wg.Wait()
}

func benchmarkHTTPBatch(batchSize int, b *testing.B) {
	var wg sync.WaitGroup
	var components []interface{}
	var workers []int

	f := NewRBForwarder(Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: 10000,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			TimeoutMillis: 1000,
			Limit:         uint64(batchSize),
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
		"http_endpoint": "test",
		"batch_group":   "test",
	}

	wg.Add(1)
	b.ResetTimer()
	go func() {
		for report := range f.GetReports() {
			r := report.(Report)
			if r.Code > 0 {
				b.FailNow()
			}
			if r.Opaque.(int) >= b.N-1 {
				break
			}
		}

		wg.Done()
	}()

	for i := 0; i < b.N; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	wg.Wait()
}

func benchmarkHTTP(b *testing.B) {
	var wg sync.WaitGroup
	var components []interface{}
	var workers []int

	f := NewRBForwarder(Config{
		Retries:   3,
		Backoff:   5,
		QueueSize: 10000,
	})

	sender := &httpsender.HTTPSender{
		URL:    "http://localhost:8888",
		Client: NewTestClient(200, func(r *http.Request) {}),
	}
	components = append(components, sender)
	workers = append(workers, 1)

	f.PushComponents(components, workers)
	f.Run()

	opts := map[string]interface{}{
		"http_endpoint": "test",
	}

	wg.Add(1)
	b.ResetTimer()
	go func() {
		for report := range f.GetReports() {
			r := report.(Report)
			if r.Code > 0 {
				b.FailNow()
			}
			if r.Opaque.(int) >= b.N-1 {
				break
			}
		}

		wg.Done()
	}()

	for i := 0; i < b.N; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	wg.Wait()
}

////////////////////////////////////////////////////////////////////////////////
/// Aux functions
////////////////////////////////////////////////////////////////////////////////

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
