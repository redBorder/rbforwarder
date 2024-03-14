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

// +nobuild examples

package main

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/x-cray/logrus-prefixed-formatter"

	"github.com/redBorder/rbforwarder"
	"github.com/redBorder/rbforwarder/components/batch"
	"github.com/redBorder/rbforwarder/components/httpsender"
	"github.com/redBorder/rbforwarder/components/limiter"
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

	// Add Limiter component
	components = append(components, &limiter.Limiter{
		Config: limiter.Config{
			BytesLimit: 256 * 1024,
		},
	})

	// Add Batch component
	components = append(components, &batcher.Batcher{
		Config: batcher.Config{
			Workers:       2,
			TimeoutMillis: 100,
			Limit:         10000,
		},
	})

	// Add HTTP Sender component
	components = append(components, &httpsender.HTTPSender{
		Config: httpsender.Config{
			Workers: 10,
			Logger:  httpLogger,
			URL:     "http://localhost:8888",
		},
	})

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
