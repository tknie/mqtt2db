/*
* Copyright 2023 Thorsten A. Knieling
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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/sirupsen/logrus"
	"github.com/tknie/flynn"
	"github.com/tknie/flynn/common"
	tlog "github.com/tknie/log"
	"github.com/tknie/services"
)

const layout = "2006-01-02T15:04:05"

type event struct {
	Time      time.Time `json:"Time"`
	Total     float64   `json:"total_in"`
	PowerCurr int64     `json:"Power_curr"`
}

var dbid common.RegDbID
var tableName string
var logRus = logrus.StandardLogger()

func init() {
	tableName = os.Getenv("MQTT_STORE_TABLENAME")
	startLog()
}

func startLog() {
	services.ServerMessage("Init logging")
	fileName := "db.trace.log"
	level := os.Getenv("ENABLE_DB_DEBUG")
	logLevel := logrus.WarnLevel
	switch level {
	case "debug", "1":
		tlog.SetDebugLevel(true)
		logLevel = logrus.DebugLevel
	case "info", "2":
		tlog.SetDebugLevel(false)
		logLevel = logrus.InfoLevel
	default:
	}
	logRus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05",
	})
	logRus.SetLevel(logLevel)
	p := os.Getenv("LOGPATH")
	if p == "" {
		p = os.TempDir()
	}

	path := filepath.FromSlash(p + string(os.PathSeparator) + fileName)
	f, err := os.OpenFile(path,
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening log:", err)
		return
	}
	logRus.SetOutput(f)
	logRus.Infof("Init logrus")
	tlog.Log = logRus
	services.ServerMessage("Logging initiated ...")
}

func main() {
	server := ""
	topic := ""
	var qos int
	var clientid string
	username := ""
	password := ""

	flag.StringVar(&server, "server", "", "The MQTT server to connect to ex: 127.0.0.1:1883")
	flag.StringVar(&topic, "topic", "", "Topic to subscribe to")
	flag.IntVar(&qos, "qos", 0, "The QoS to subscribe to messages at")
	flag.StringVar(&clientid, "clientid", "", "A clientid for the connection")
	flag.StringVar(&username, "username", "", "A username to authenticate to the MQTT server")
	flag.StringVar(&password, "password", "", "Password to match username")
	flag.Parse()

	if topic == "" {
		topic = os.Getenv("MQTT_TOPIC")
	}
	if topic == "" {
		topic = "#"
	}
	if server == "" {
		server = os.Getenv("MQTT_TOPIC_URL")
	}
	if username == "" {
		username = os.Getenv("MQTT_TOPIC_USERNAME")
	}
	if password == "" {
		password = os.Getenv("MQTT_TOPIC_PASSWORD")
	}

	services.ServerMessage("MQTT server: %s", server)
	services.ServerMessage("MQTT topic: %s", topic)
	services.ServerMessage("MQTT username: %s", username)

	initDatabase()
	defer close()

	logger := &logger{}
	msgChan := make(chan *paho.Publish)

	services.ServerMessage("Connecting ... %s", server)
	conn, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatalf("Failed to dial to %s: %s", server, err)
	}

	c := paho.NewClient(paho.ClientConfig{
		Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
			msgChan <- m
		}),
		Conn: conn,
	})
	c.SetDebugLogger(logger)
	c.SetErrorLogger(logger)

	services.ServerMessage("Connecting paho")
	cp := &paho.Connect{
		KeepAlive:  30,
		ClientID:   clientid,
		CleanStart: true,
		Username:   username,
		Password:   []byte(password),
	}

	if username != "" {
		cp.UsernameFlag = true
	}
	if password != "" {
		cp.PasswordFlag = true
	}

	ca, err := c.Connect(context.Background(), cp)
	if err != nil {
		log.Fatalln(err)
	}
	if ca.ReasonCode != 0 {
		log.Fatalf("Failed to connect to %s : %d - %s", server, ca.ReasonCode, ca.Properties.ReasonString)
	}

	services.ServerMessage("Connecting MQTT to %s", server)

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

	subscriptions := make(map[string]paho.SubscribeOptions)
	subscriptions[topic] = paho.SubscribeOptions{QoS: byte(qos)}

	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: subscriptions,
	})
	if err != nil {
		log.Fatalln(err)
	}
	if sa.Reasons[0] != byte(qos) {
		log.Fatalf("Failed to subscribe to %s : %d", topic, sa.Reasons[0])
	}
	services.ServerMessage("Subscribed MQTT to %s", topic)
	defer services.ServerMessage("MQTT exited")

	counter := uint64(0)
	for m := range msgChan {
		tlog.Log.Debugf("%s: Message: %s", m.Topic, string(m.Payload))
		x := make(map[string]interface{})
		tlog.Log.Debugf("EVENT....")
		err := json.Unmarshal(m.Payload, &x)
		if err != nil {
			fmt.Println("JSON unmarshal fails:", err)
		}
		t, err := time.ParseInLocation(layout, x["Time"].(string), time.Local)
		//t, err := time.Parse(layout, x["Time"].(string))
		if err == nil {
			e := &event{Time: t.UTC()}
			insert := &common.Entries{DataStruct: e,
				Fields: []string{"*"}}
			m := x[""].(map[string]interface{})
			e.PowerCurr = int64(m["Power_curr"].(float64))
			e.Total = m["total_in"].(float64)
			err = dbid.Insert(tableName, insert)
			if err != nil {
				log.Fatal("Error inserting record", err)
			}
			fmt.Print(".")
			if counter%60 == 0 {
				fmt.Println()
			}
			counter++
		}
	}
}

func initDatabase() {
	url := os.Getenv("MQTT_STORE_URL")
	if url == "" || tableName == "" {
		log.Fatal("Table parameter not defined...")
	}
	dbRef, password, err := common.NewReference(url)
	if err != nil {
		log.Fatal("REST audit URL incorrect: " + url)
	}
	if password == "" {
		password = os.Getenv("MQTT_STORE_PASS")
	}
	dbRef.User = os.Getenv("MQTT_STORE_USER")
	if dbRef.User == "" {
		dbRef.User = "admin"
	}

	services.ServerMessage("Storing MQTT data to table '%s'", tableName)
	id, err := flynn.RegisterDatabase(dbRef, password)
	if err != nil {
		log.Fatalf("Register error log: %v", err)
	}

	status, err := id.CreateTableIfNotExists(tableName, &event{})
	if err != nil {
		log.Fatalf("Database log creating failed: %v %T", err, status)
	}
	dbid = id
	services.ServerMessage("Database initiated")
}

func close() {
	defer flynn.Unregister(dbid)
}

type logger struct {
}

func (l *logger) Println(v ...interface{}) {
	s := ""
	for range v {
		s += "%v "
	}
	s += "\n"
	tlog.Log.Debugf(s, v...)
}
func (l *logger) Printf(format string, v ...interface{}) {
	tlog.Log.Debugf(format, v...)
}
