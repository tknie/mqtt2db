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
	"os"
	"strconv"

	"github.com/tknie/log"
	"github.com/tknie/mqtt2db"
	"github.com/tknie/services"
)

const defaultMaxTries = 10

func init() {
	services.ServerMessage("Start MQTT2DB application %s (build at %s)", mqtt2db.BuildVersion, mqtt2db.BuildDate)

	log.InitZapLogWithFilename("mqtt2db.log")

}

func main() {
	sync := ""
	config := mqtt2db.Config{}
	username := ""
	password := ""

	config.MapFile = os.ExpandEnv(os.Getenv("MQTT2DB_MAPFILE"))

	flag.IntVar(&config.Qos, "qos", 0, "The QoS to subscribe to messages at")
	flag.IntVar(&config.MaxTries, "maxtries", defaultMaxTries, "The QoS to subscribe to messages at")
	flag.StringVar(&config.Clientid, "clientid", "", "A clientid for the connection")
	flag.StringVar(&username, "username", "", "A username to authenticate to the MQTT server")
	flag.StringVar(&password, "password", "", "Password to match username")
	flag.StringVar(&config.MapFile, "m", config.MapFile, "Define event mapping file")
	flag.BoolVar(&config.Create, "create", false, "Create new database")
	flag.StringVar(&sync, "s", "", "Sync to new database")
	flag.BoolVar(&mqtt2db.CloseIfStuck, "T", false, "Close if in received MQTT loop no messages received")
	flag.IntVar(&mqtt2db.OutLoopSeconds, "rm", mqtt2db.DefaultLoopSeconds, "Output Received MQTT loop and check cancel")

	flag.Parse()

	if config.Init() != nil {
		mqtt2db.InitUrl()
	}

	if sync != "" {
		services.ServerMessage("Synchronize databases...")
		mqtt2db.SyncDatabase(sync)
		return
	}

	config.LoadDefaults(username, password)

	create := os.Getenv("MQTT2DB_CREATE")
	createBool, err := strconv.ParseBool(create)
	if err == nil && createBool {
		config.Create = true
	}

	config.InitDatabase()
	defer mqtt2db.Close()

	config.ConnectMQTT()
	defer services.ServerMessage("MQTT exiting")

}
