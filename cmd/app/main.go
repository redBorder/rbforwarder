package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"

	"gopkg.in/yaml.v2"

	"github.com/Sirupsen/logrus"
	"github.com/redBorder/rbforwarder"
)

var (
	configFile    *string
	debug         *bool
	workersFlag   *int
	queueSizeFlag *int
	retriesFlag   *int
)

type config struct {
	Source    map[string]interface{} `yaml:"source"`
	Decoder   map[string]interface{} `yaml:"decoder"`
	Processor map[string]interface{} `yaml:"processor"`
	Encoder   map[string]interface{} `yaml:"encoder"`
	Sender    map[string]interface{} `yaml:"sender"`
}

func init() {
	configFile = flag.String("config", "", "Config file")
	debug = flag.Bool("debug", false, "Show debug info")
	workersFlag = flag.Int("workers", 1, "Number of workers")
	queueSizeFlag = flag.Int("queue_size", 1000, "Max number of messages in queue")
	retriesFlag = flag.Int("msg_retries", 0, "Max number of retries for a message")

	flag.Parse()

	if len(*configFile) == 0 {
		fmt.Println("No config file provided")
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	_, err := loadConfigFile(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	if *debug {
		rbforwarder.LogLevel(logrus.DebugLevel)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Create forwarder
	forwarder := rbforwarder.NewRBForwarder(rbforwarder.Config{
		Workers:   *workersFlag,
		Retries:   *retriesFlag,
		QueueSize: *queueSizeFlag,
	})

	go func() {
		<-c
		forwarder.Close()
	}()

	forwarder.Start()
}

func loadConfigFile(fileName string) (config config, err error) {
	configData, err := ioutil.ReadFile(fileName)
	if err != nil {
		return
	}

	if err = yaml.Unmarshal([]byte(configData), &config); err != nil {
		return
	}

	return
}
