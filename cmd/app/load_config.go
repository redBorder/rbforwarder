package main

import (
	"io/ioutil"

	"github.com/redBorder/rbforwarder"

	"gopkg.in/yaml.v2"
)

func loadConfigFile(fileName string) (config rbforwarder.Config, err error) {
	configData, err := ioutil.ReadFile(fileName)
	if err != nil {
		return
	}

	if err = yaml.Unmarshal([]byte(configData), &config); err != nil {
		return
	}

	return
}
