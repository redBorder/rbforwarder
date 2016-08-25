// +nobuild examples

package main

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/x-cray/logrus-prefixed-formatter"

	"github.com/redBorder/rbforwarder"
	"github.com/redBorder/rbforwarder/components/batch"
	"github.com/redBorder/rbforwarder/components/httpsender"
)

func main() {
	var wg sync.WaitGroup
	var components []interface{}
	const numMessages = 100000

	httpLogger := logrus.New().WithField("prefix", "HTTP Sender")
	httpLogger.Logger.Formatter = new(prefixed.TextFormatter)

	f := rbforwarder.NewRBForwarder(rbforwarder.Config{
		Retries:   2,
		Backoff:   15,
		QueueSize: 10,
	})

	batch := &batcher.Batcher{
		Config: batcher.Config{
			Workers:       2,
			TimeoutMillis: 100,
			Limit:         10000,
		},
	}
	components = append(components, batch)

	sender := &httpsender.HTTPSender{
		Config: httpsender.Config{
			Workers: 10,
			Logger:  httpLogger,
			Debug:   true,
			URL:     "http://localhost:8888",
		},
	}
	components = append(components, sender)

	f.PushComponents(components)

	opts := map[string]interface{}{
		"http_endpoint": "test",
		"batch_group":   "example",
	}

	f.Run()

	wg.Add(1)
	go func() {
		var errors int
		var messages int

		fmt.Print("[")
		for report := range f.GetOrderedReports() {
			r := report.(rbforwarder.Report)
			// fmt.Printf("[MESSAGE %d] %s\n", r.Opaque.(int), r.Status)
			messages++
			if messages%(numMessages/20) == 0 {
				fmt.Printf("=")
			}
			if r.Code > 0 {
				errors++
			}
			if messages >= numMessages {
				break
			}
		}
		fmt.Print("] ")
		fmt.Printf("Sent %d messages with %d errors\n", messages, errors)
		wg.Done()
	}()

	for i := 0; i < numMessages; i++ {
		data := fmt.Sprintf("{\"message\": %d}", i)
		f.Produce([]byte(data), opts, i)
	}

	wg.Wait()
}
