package util

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

func LoadConfigFile(fileName string) (config PipeConfig, err error) {
	configData, err := ioutil.ReadFile(fileName)
	if err != nil {
		return
	}

	err = yaml.Unmarshal([]byte(configData), &config)
	if err != nil {
		return
	}

	return
}
