package main

import (
	"flag"
	"fmt"
	"github.com/Sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"

	"github.com/Bigomby/go-pipes/pipe"
	"github.com/Bigomby/go-pipes/util"
)

var mainLog *logrus.Entry
var profile bool

var (
	configFile string
	workers    int
)

func init() {
	configFileFlag := flag.String("config", "", "Config file")
	debugFlag := flag.Bool("debug", false, "Show debug info")
	profileFlag := flag.Bool("profile", false, "Profile CPU usage")
	workersFlag := flag.Int("workers", 1, "Number of workers")

	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	workers = *workersFlag

	if *debugFlag {
		util.Level = logrus.DebugLevel
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
	}

	if *profileFlag {
		profile = true
	}

	if len(*configFileFlag) == 0 {
		fmt.Println("No config file provided")
		flag.Usage()
		os.Exit(1)
	}

	configFile = *configFileFlag
}

func main() {
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, os.Interrupt)

	mainLog = util.NewLogger("main")

	if profile {
		f, err := os.Create("cpu_profile")
		if err != nil {
			mainLog.Fatal(err)
		}
		mainLog.Info("Starting CPU profiling")
		pprof.StartCPUProfile(f)
		defer func() {
			mainLog.Info("Stoping CPU profiling")
			pprof.StopCPUProfile()
		}()
	}

	config, err := util.LoadConfigFile(configFile)
	if err != nil {
		mainLog.Fatal(err)
	}

	mainLog.Infof("Loaded config file: %s", configFile)

	pipe.NewPipe(config, workers)
	mainLog.Infof("Using %d workers", workers)
}
