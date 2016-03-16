package main

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"

	"github.com/Sirupsen/logrus"
	"github.com/redBorder/rbforwarder"
	"github.com/redBorder/rbforwarder/senders"
	"github.com/redBorder/rbforwarder/sources"
)

var (
	configFile    *string
	debug         *bool
	workersFlag   *int
	queueSizeFlag *int
)

func init() {
	configFile = flag.String("config", "", "Config file")
	debug = flag.Bool("debug", false, "Show debug info")
	workersFlag = flag.Int("workers", 1, "Number of workers")
	queueSizeFlag = flag.Int("queue_size", 1000, "Max number of messages in queue")

	flag.Parse()

	if len(*configFile) == 0 {
		fmt.Println("No config file provided")
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	config, err := loadConfigFile(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	if *debug {
		rbforwarder.LogLevel(logrus.DebugLevel)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	forwarder := rbforwarder.NewRBForwarder(*workersFlag, *queueSizeFlag)

	go func() {
		<-c
		forwarder.Close()
	}()

	forwarder.SetListener(sources.NewListener(config.Listener))
	forwarder.SetSender(senders.NewSender(config.Sender))

	forwarder.Start()
}
