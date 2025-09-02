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

package main

import (
	"flag"

	"github.com/tknie/mqtt2db"
	"github.com/tknie/services"
)

// BuildDate build date
var BuildDate string

// BuildVersion build version
var BuildVersion string

const defaultMaxTries = 10

func init() {
	services.ServerMessage("Start MQTT2DB application %s (build at %s)", BuildVersion, BuildDate)

	mqtt2db.StartLog()
}

func main() {
	config := mqtt2db.Config{}
	flag.StringVar(&config.Server, "server", "", "The MQTT server to connect to ex: 127.0.0.1:1883")
	flag.StringVar(&config.Topic, "topic", "", "Topic to subscribe to")
	flag.IntVar(&config.Qos, "qos", 0, "The QoS to subscribe to messages at")
	flag.IntVar(&config.MaxTries, "maxtries", defaultMaxTries, "The QoS to subscribe to messages at")
	flag.StringVar(&config.Clientid, "clientid", "", "A clientid for the connection")
	flag.StringVar(&config.Username, "username", "", "A username to authenticate to the MQTT server")
	flag.StringVar(&config.Password, "password", "", "Password to match username")
	flag.BoolVar(&config.Create, "create", false, "Create new database")
	flag.Parse()

	config.LoadDefaults()

	mqtt2db.InitDatabase(config.Create, config.MaxTries)
	defer mqtt2db.Close()

	config.ConnectMQTT()
	defer services.ServerMessage("MQTT exiting")

}
