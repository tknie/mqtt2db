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

// loop loop through receiving all messages from MQTT and store them into
// the database
func loopIncomingMessages(msgChan chan *paho.Publish) {
	counter := uint64(0)
	for m := range msgChan {
		tlog.Log.Debugf("%s: Message: %s", m.Topic, string(m.Payload))
		x := make(map[string]interface{})
		tlog.Log.Debugf("EVENT....%s", string(m.Payload))
		err := json.Unmarshal(m.Payload, &x)
		if err != nil {
			fmt.Println("JSON unmarshal fails:", err)
			continue
		}
		// parse in location for local TZ
		t, err := time.ParseInLocation(layout, x["Time"].(string), time.Local)
		if err == nil {
			counter++
			em := make(map[string]interface{})
			em["Time"] = t.UTC()
			e := &event{Time: t.UTC()}

			// Below is the corresponding structure transfered into the structure
			// Parsing into structure fails because of different topics
			// Please adapt the x[] map reference if structure differs
			var o interface{}
			var ok bool
			if o, ok = x["eHZ"]; !ok {
				fmt.Println("Error search 'eHZ'")
				continue
			}

			m := o.(map[string]interface{})
			if o, ok = m["Power"]; !ok {
				fmt.Println("Error search 'Power'")
				continue
			}
			em["PowerCurr"] = int64(o.(float64))
			e.PowerCurr = int64(o.(float64))
			if o, ok = m["E_in"]; !ok {
				fmt.Println("Error search 'E_in'")
				return
			}
			e.Total = o.(float64)
			em["Total"] = o.(float64)
			if o, ok = m["E_out"]; !ok {
				fmt.Println("Error search 'E_in'")
				return
			}
			e.PowerOut = o.(float64)
			em["PowerOut"] = o.(float64)
			if e.PowerCurr < 0 && e.PowerOut == 0 {
				e.PowerOut = float64(-e.PowerCurr)
				e.PowerCurr = 0
				em["PowerOut"] = float64(-em["PowerCurr"].(int64))
				em["PowerCurr"] = 0
			}
			storeEvent(em)
			if counter%350 == 0 {
				services.ServerMessage("Received MQTT msgs: %04d ->  %v", counter, time.Now())
			}
			os.Stdout.Sync()
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

	services.ServerMessage("Connect TCP/IP to %s", config.Server)
	conn := tryConnectMQTT(config.Server, config.MaxTries)

	c := paho.NewClient(paho.ClientConfig{PacketTimeout: 2 * time.Minute,
		Router: paho.NewStandardRouterWithDefault(func(m *paho.Publish) {
			msgChan <- m
		}),
		Conn: conn,
	})
	c.SetDebugLogger(logger)
	c.SetErrorLogger(logger)

	services.ServerMessage("Connecting paho services")

	// connect to MQTT and listen and subscribe
	cp := &paho.Connect{
		KeepAlive:  30,
		ClientID:   config.Clientid,
		CleanStart: true,
		Username:   config.Username,
		Password:   []byte(config.Password),
	}

	if config.Username != "" {
		cp.UsernameFlag = true
	}
	if config.Password != "" {
		cp.PasswordFlag = true
	}

	// connecting to MQTT server
	ca, err := c.Connect(context.Background(), cp)
	if err != nil {
		log.Fatalln(err)
	}
	if ca.ReasonCode != 0 {
		log.Fatalf("Failed to connect to %s : %d - %s", config.Server, ca.ReasonCode, ca.Properties.ReasonString)
	}

	services.ServerMessage("Connecting MQTT to %s", config.Server)

	ic := make(chan os.Signal, 1)
	signal.Notify(ic, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ic
		fmt.Println("signal received, exiting")
		if c != nil {
			d := &paho.Disconnect{ReasonCode: 0}
			c.Disconnect(d)
		}
		os.Exit(0)
	}()

	// subscribe to a subscription MQTT topic
	subscriptions := make([]paho.SubscribeOptions, 0)
	subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: config.Topic,
		QoS: byte(config.Qos)})

	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: subscriptions,
	})
	if err != nil {
		services.ServerMessage("Error subscribing MQTT ... %v", err)
		log.Fatalln(err)
	}
	if sa.Reasons[0] != byte(config.Qos) {
		log.Fatalf("Failed to subscribe to %s : %d", config.Topic, sa.Reasons[0])
	}
	services.ServerMessage("Subscribed MQTT to %s", config.Topic)
	loopIncomingMessages(msgChan)
}
