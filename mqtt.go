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
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/paho"
	tlog "github.com/tknie/log"
	"github.com/tknie/services"
)

const layout = "2006-01-02T15:04:05"

type event struct {
	Time      time.Time `json:"Time"`
	Total     float64   `json:"total_in"`
	PowerCurr int64     `json:"Power_curr"`
	PowerOut  float64   `json:"Powerout"`
}

var counter = uint64(0)
var mqttDone = make(chan bool, 1)

const DefaultLoopSeconds = 120

var OutLoopSeconds = 120
var CloseIfStuck = false

// loop loop through receiving all messages from MQTT and store them into
// the database
func loopIncomingMessages(msgChan chan *paho.Publish) {
	if OutLoopSeconds == 0 {
		return
	}
	go loopCounterAndCancelOutput()
	for m := range msgChan {
		tlog.Log.Debugf("%s: Message: %s", m.Topic, string(m.Payload))
		x := make(map[string]interface{})
		tlog.Log.Debugf("EVENT....%s", string(m.Payload))
		err := json.Unmarshal(m.Payload, &x)
		if err != nil {
			fmt.Println("JSON unmarshal fails:", err)
			continue
		}

		em := ParseMessage(x)
		if em != nil {
			storeEvent(em)
			os.Stdout.Sync()
		}
	}
}

func loopCounterAndCancelOutput() {
	lastCounter := uint64(0)
	try := 0
	for {
		select {
		case <-mqttDone:
			services.ServerMessage("Ecoflow analyze loop is stopped")
			return
		case <-time.After(time.Second * time.Duration(OutLoopSeconds)):
			services.ServerMessage("Received MQTT msgs: %04d", counter)
			if counter == lastCounter && CloseIfStuck {
				if try > 10 {
					services.ServerMessage("Received MQTT msgs error still stuck")
					os.Exit(10)
				}
				try++
			} else {
				try = 0
			}
			lastCounter = counter
		}
	}
}

func tryConnectMQTT(server string, tries int) net.Conn {
	var err error
	var conn net.Conn
	for count := 0; count < tries; count++ {
		conn, err = net.Dial("tcp", server)
		if err == nil {
			return conn
		}
		if count < tries {
			services.ServerMessage("Error connecting MQTT retrying soon ... %v", err)
			time.Sleep(10 * time.Second)
		} else {
			services.ServerMessage("Error connecting MQTT ... %v", err)
		}
	}
	if err != nil {
		log.Fatalf("Failed to dial to %s: %s", server, err)
	}
	return nil
}

func (config *Config) ConnectMQTT() {
	logger := &MQTTWrapperLogger{}
	msgChan := make(chan *paho.Publish)

	services.ServerMessage("Connect TCP/IP to %s", c.Mqtt.Server)
	conn := tryConnectMQTT(c.Mqtt.Server, config.MaxTries)

	pahoClient := paho.NewClient(paho.ClientConfig{PacketTimeout: 2 * time.Minute,
		Router: paho.NewStandardRouterWithDefault(func(m *paho.Publish) {
			msgChan <- m
		}),
		Conn: conn,
	})
	pahoClient.SetDebugLogger(logger)
	pahoClient.SetErrorLogger(logger)

	services.ServerMessage("Connecting paho services to %s", c.Mqtt.Server)
	password := os.ExpandEnv(c.Mqtt.Password)

	// connect to MQTT and listen and subscribe
	cp := &paho.Connect{
		KeepAlive:  30,
		ClientID:   config.Clientid,
		CleanStart: true,
		Username:   c.Mqtt.Username,
		Password:   []byte(password),
	}

	if c.Mqtt.Username != "" {
		cp.UsernameFlag = true
	}
	if password != "" {
		cp.PasswordFlag = true
	}

	// connecting to MQTT server
	ca, err := pahoClient.Connect(context.Background(), cp)
	if err != nil {
		services.ServerMessage("Error to connect paho services to %s with %s: %v",
			c.Mqtt.Server, c.Mqtt.Username, err)
		log.Fatalln(err)
	}
	if ca.ReasonCode != 0 {
		services.ServerMessage("Failed to connect paho services to %s with %s with reason code %d",
			c.Mqtt.Server, c.Mqtt.Username, ca.ReasonCode)

		log.Fatalf("Failed to connect to %s : %d - %s", c.Mqtt.Server, ca.ReasonCode, ca.Properties.ReasonString)
	}

	services.ServerMessage("Connecting MQTT to %s", c.Mqtt.Server)

	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ic
		fmt.Println("signal received, exiting")
		if c != nil {
			d := &paho.Disconnect{ReasonCode: 0}
			pahoClient.Disconnect(d)
		}
		os.Exit(0)
	}()

	// subscribe to a subscription MQTT topic
	subscriptions := make([]paho.SubscribeOptions, 0)
	subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: c.Mqtt.Topic,
		QoS: byte(config.Qos)})

	sa, err := pahoClient.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: subscriptions,
	})
	if err != nil {
		services.ServerMessage("Error subscribing MQTT ... %v", err)
		log.Fatalln(err)
	}
	if sa.Reasons[0] != byte(config.Qos) {
		log.Fatalf("Failed to subscribe to %s : %d", c.Mqtt.Topic, sa.Reasons[0])
	}
	services.ServerMessage("Subscribed MQTT to %s", c.Mqtt.Topic)
	loopIncomingMessages(msgChan)
}
