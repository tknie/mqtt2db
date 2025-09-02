/*
* Copyright 2023-2025 Thorsten A. Knieling
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
 */

package mqtt2db

import (
	"os"

	"github.com/tknie/services"
)

type Config struct {
	Server   string
	Topic    string
	Qos      int
	Clientid string
	Username string
	Password string
	Create   bool

	MaxTries int
}

func (config *Config) LoadDefaults() {
	if config.Topic == "" {
		config.Topic = os.Getenv("MQTT_TOPIC")
	}
	if config.Topic == "" {
		config.Topic = "#"
	}
	if config.Server == "" {
		config.Server = os.Getenv("MQTT_TOPIC_URL")
	}
	if config.Username == "" {
		config.Username = os.Getenv("MQTT_TOPIC_USERNAME")
	}
	if config.Password == "" {
		config.Password = os.Getenv("MQTT_TOPIC_PASSWORD")
	}

	services.ServerMessage("MQTT server: %s", config.Server)
	services.ServerMessage("MQTT topic: %s", config.Topic)
	services.ServerMessage("MQTT username: %s", config.Username)

}
