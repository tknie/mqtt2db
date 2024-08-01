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
	fmt.Println("Init logging")
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
	f, err := os.OpenFile(p+"/"+fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening log:", err)
		return
	}
	logRus.SetOutput(f)
	logRus.Infof("Init logrus")
	tlog.Log = logRus
	fmt.Println("Logging running")
}

func main() {
	server := flag.String("server", "127.0.0.1:1883", "The MQTT server to connect to ex: 127.0.0.1:1883")
	topic := flag.String("topic", "#", "Topic to subscribe to")
	qos := flag.Int("qos", 0, "The QoS to subscribe to messages at")
	clientid := flag.String("clientid", "", "A clientid for the connection")
	username := flag.String("username", "", "A username to authenticate to the MQTT server")
	password := flag.String("password", "", "Password to match username")
	flag.Parse()

	initDatabase()
	defer close()

	logger := &logger{}
	msgChan := make(chan *paho.Publish)

	conn, err := net.Dial("tcp", *server)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %s", *server, err)
	}

	c := paho.NewClient(paho.ClientConfig{
		Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
			msgChan <- m
		}),
		Conn: conn,
	})
	c.SetDebugLogger(logger)
	c.SetErrorLogger(logger)

	cp := &paho.Connect{
		KeepAlive:  30,
		ClientID:   *clientid,
		CleanStart: true,
		Username:   *username,
		Password:   []byte(*password),
	}

	if *username != "" {
		cp.UsernameFlag = true
	}
	if *password != "" {
		cp.PasswordFlag = true
	}

	ca, err := c.Connect(context.Background(), cp)
	if err != nil {
		log.Fatalln(err)
	}
	if ca.ReasonCode != 0 {
		log.Fatalf("Failed to connect to %s : %d - %s", *server, ca.ReasonCode, ca.Properties.ReasonString)
	}

	fmt.Printf("Connected to %s\n", *server)

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
	subscriptions[*topic] = paho.SubscribeOptions{QoS: byte(*qos)}

	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: subscriptions,
	})
	if err != nil {
		log.Fatalln(err)
	}
	if sa.Reasons[0] != byte(*qos) {
		log.Fatalf("Failed to subscribe to %s : %d", *topic, sa.Reasons[0])
	}
	log.Printf("Subscribed to %s", *topic)

	for m := range msgChan {
		log.Println(m.Topic, ": Message:", string(m.Payload))
		x := make(map[string]interface{})
		fmt.Println("EVENT....")
		// {"sn":{"Time":"2024-08-01T22:14:45","eHZ":{"E_in":16585.289,"E_out":0.000,"Power":326}},"ver":1}
		err := json.Unmarshal(m.Payload, &x)
		if err != nil {
			fmt.Println("JSON unmarshal fails:", err)
		} else {
			for n, v := range x {
				fmt.Println(n, v)
			}
		}
		t, err := time.Parse(layout, x["Time"].(string))
		if err == nil {
			e := &event{Time: t}
			insert := &common.Entries{DataStruct: e,
				Fields: []string{"*"}}
			m := x["eHZ"].(map[string]interface{})
			e.PowerCurr = int64(m["Power"].(float64))
			e.Total = m["E_in"].(float64)
			err = dbid.Insert(tableName, insert)
			if err != nil {
				log.Fatal("Error inserting record", err)
			}
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
	dbRef.User = "admin"

	services.ServerMessage("Storing MQTT data to table '%s'", tableName)
	id, err := flynn.RegisterDatabase(dbRef, password)
	if err != nil {
		log.Fatalf("Register error log: %v", err)
	}

	status, err := id.CreateTableIfNotExists(tableName, &event{})
	if err != nil {
		log.Fatalf("Databaase log creating failed: %v %T", err, status)
	}
	dbid = id
}

func close() {
	defer flynn.Unregister(dbid)
}

type logger struct {
}

func (l *logger) Println(v ...interface{}) {

}
func (l *logger) Printf(format string, v ...interface{}) {

}
