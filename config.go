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
	Qos        int
	Clientid   string
	MapFile    string
	Create     bool
	TrackInput int
	MaxTries   int
}

func (config *Config) LoadDefaults(username, password string) {
	if c.Mqtt.Topic == "" {
		c.Mqtt.Topic = os.Getenv("MQTT_TOPIC")
	}
	if c.Mqtt.Topic == "" {
		c.Mqtt.Topic = "#"
	}
	if c.Mqtt.Server == "" {
		c.Mqtt.Server = os.Getenv("MQTT_TOPIC_URL")
	}
	if username != "" {
		c.Mqtt.Username = username
	} else if c.Mqtt.Username == "" {
		c.Mqtt.Username = os.Getenv("MQTT_TOPIC_USERNAME")
	}
	if password != "" {
		c.Mqtt.Password = password
	} else if c.Mqtt.Password == "" {
		c.Mqtt.Password = os.Getenv("MQTT_TOPIC_PASSWORD")
	}
	if c.Database.StoreTablename == "" {
		c.Database.StoreTablename = os.Getenv("MQTT_STORE_TABLENAME")
	}

	services.ServerMessage("MQTT server: %s", c.Mqtt.Server)
	services.ServerMessage("MQTT topic: %s", c.Mqtt.Topic)
	services.ServerMessage("MQTT username: %s", c.Mqtt.Username)

}
