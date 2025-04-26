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

// BuildDate build date
var BuildDate string

// BuildVersion build version
var BuildVersion string

const layout = "2006-01-02T15:04:05"

const defaultMaxTries = 10

var maxTries = defaultMaxTries

type event struct {
	Time      time.Time `json:"Time"`
	Total     float64   `json:"total_in"`
	PowerCurr int64     `json:"Power_curr"`
	PowerOut  float64   `json:"Powerout"`
}

var dbid common.RegDbID
var tableName string
var logRus = logrus.StandardLogger()

var SQLbatches = []string{
	`ALTER TABLE public.home ADD inserted_on timestamptz NULL;`,
	`CREATE OR REPLACE FUNCTION public.update_homefct_timestamp()
			RETURNS trigger
			LANGUAGE plpgsql
		   AS $function$
		   BEGIN
				  NEW.inserted_on = CURRENT_TIMESTAMP;
				  RETURN NEW;
		   END;
		   $function$
		   ;`,
	`ALTER FUNCTION public.update_timestamp() OWNER TO postgres;`,
	`GRANT ALL ON FUNCTION public.update_timestamp() TO postgres;`,
	`create or replace trigger update_home_timestamp before
		   insert
		   on
		   public.home for each row execute function update_homefct_timestamp();`,
	`CREATE INDEX home_inserted_on_idx ON public.home USING btree (inserted_on);`,
	`CREATE INDEX home_inserted_on_idx_desc ON public.home USING btree (inserted_on DESC);`,
	`ALTER TABLE public.home ADD id serial4 NOT NULL;`}

func init() {
	services.ServerMessage("MQTT2DB version %s (build at %s)", BuildVersion, BuildDate)

	tableName = os.Getenv("MQTT_STORE_TABLENAME")
	startLog()
}

func startLog() {
	fileName := "mqtt2db.trace.log"
	level := os.Getenv("ENABLE_MQTT2DB_DEBUG")
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
	create := false

	flag.StringVar(&server, "server", "", "The MQTT server to connect to ex: 127.0.0.1:1883")
	flag.StringVar(&topic, "topic", "", "Topic to subscribe to")
	flag.IntVar(&qos, "qos", 0, "The QoS to subscribe to messages at")
	flag.IntVar(&maxTries, "maxtries", defaultMaxTries, "The QoS to subscribe to messages at")
	flag.StringVar(&clientid, "clientid", "", "A clientid for the connection")
	flag.StringVar(&username, "username", "", "A username to authenticate to the MQTT server")
	flag.StringVar(&password, "password", "", "Password to match username")
	flag.BoolVar(&create, "create", false, "Create new database")
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

	initDatabase(create)
	defer close()

	logger := &logger{}
	msgChan := make(chan *paho.Publish)

	services.ServerMessage("Connect TCP/IP to %s", server)
	conn := tryConnectMQTT(server, maxTries)

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

	// connecting to MQTT server
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

	// subscribe to a subscription MQTT topic
	subscriptions := make([]paho.SubscribeOptions, 0)
	subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: topic,
		QoS: byte(qos)})

	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: subscriptions,
	})
	if err != nil {
		services.ServerMessage("Error subscribing MQTT ... %v", err)
		log.Fatalln(err)
	}
	if sa.Reasons[0] != byte(qos) {
		log.Fatalf("Failed to subscribe to %s : %d", topic, sa.Reasons[0])
	}
	services.ServerMessage("Subscribed MQTT to %s", topic)
	defer services.ServerMessage("MQTT exiting")
	loopIncomingMessages(msgChan)
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
			e := &event{Time: t.UTC()}
			insert := &common.Entries{DataStruct: e,
				Fields: []string{"*"}}
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
			e.PowerCurr = int64(o.(float64))
			if o, ok = m["E_in"]; !ok {
				fmt.Println("Error search 'E_in'")
				return
			}
			e.Total = o.(float64)
			if o, ok = m["E_out"]; !ok {
				fmt.Println("Error search 'E_in'")
				return
			}
			e.PowerOut = o.(float64)
			if e.PowerCurr < 0 && e.PowerOut == 0 {
				e.PowerOut = float64(-e.PowerCurr)
				e.PowerCurr = 0
			}
			insert.Values = [][]any{{e}}
			_, err = dbid.Insert(tableName, insert)
			if err != nil {
				log.Fatal("Error inserting record: ", err)
			}
			if counter%350 == 0 {
				services.ServerMessage("Received: %04d ->  %v", counter, time.Now())
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

// initDatabase initialize database by
//   - creating storage table
//   - create index for inserted_on
//   - create function for updating inserted_on the current
//     timestamp
//   - add id serial
func initDatabase(create bool) {
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
	id, err := flynn.Handler(dbRef, password)
	if err != nil {
		log.Fatalf("Register error log: %v", err)
	}
	var status common.CreateStatus
	count := 0
	for count < maxTries {
		count++
		tlog.Log.Debugf("Try count=%d", count)

		if create {
			// create table if not exists
			status, err = id.CreateTableIfNotExists(tableName, &event{})
			if err != nil {
				if count < 10 {
					services.ServerMessage("Wait because of creation err %T: %v", status, err)
					time.Sleep(10 * time.Second)
					services.ServerMessage("Skipt counter increased to %d", count)
					continue
				} else {
					log.Fatalf("Database log creating failed: %v %T", err, status)
				}
			}
			tlog.Log.Debugf("Received status=%v", status)
			// if database is created, then call batch commands
			if status == common.CreateCreated {
				for i, batch := range SQLbatches {
					err = id.Batch(batch)
					if err != nil {
						log.Fatalf("Database batch(%03d) failed: %v", i, err)
					}
				}
			}
		}
		dbid = id

		// final ping checks if database is online
		err = id.Ping()
		if err != nil {
			services.ServerMessage("%d. Pinging failed: %v", count, err)
			time.Sleep(10 * time.Second)
			services.ServerMessage("Skip counter increased to %d", count)
		} else {
			services.ServerMessage("Pinging successfullly done")
			services.ServerMessage("Database initiated")
			return
		}
		tlog.Log.Debugf("End error=%v", err)
	}

}

// close close and unregister flynn identifier
func close() {
	defer dbid.FreeHandler()
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
