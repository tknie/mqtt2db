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
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	tlog "github.com/tknie/log"
	"github.com/tknie/services"

	"github.com/tknie/flynn/common"
	"gopkg.in/yaml.v3"
)

type Database struct {
	Url            string
	StoreTablename string `yaml:"storeTablename"`
}

type Mqtt struct {
	Server   string `yaml:"server"`
	Topic    string `yaml:"topic"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Mapping []struct {
	Source      string `yaml:"source"`
	Destination string `yaml:"destination"`
	Type        string `yaml:"type"`
}

type Mqtt2db struct {
	Database Database `yaml:"database"`
	Mqtt     Mqtt     `yaml:"mqtt"`
	Mapping  Mapping  `yaml:"mapping"`
}

var c = &Mqtt2db{}

func InitMapping(mapFile string) {
	tlog.Log.Debugf("Parsing mapping config file %s", mapFile)
	yamlFile, err := os.ReadFile(mapFile)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	initUrl()
}

func getUrl() (*common.Reference, string) {
	dbRef, password, err := common.NewReference(c.Database.Url)
	if err != nil {
		log.Fatal("REST audit URL incorrect: " + c.Database.Url)
	}
	if password == "" {
		password = os.Getenv("MQTT_STORE_PASS")
	}
	if dbRef.User == "" {
		dbRef.User = os.Getenv("MQTT_STORE_USER")
	}
	if dbRef.User == "" {
		dbRef.User = "admin"
	}
	return dbRef, password
}

func initUrl() {
	url := os.Getenv("MQTT_STORE_URL")
	if url == "" {
		if c.Database.Url == "" {
			log.Fatal("Table MQTT_STORE_URL parameter not defined...")
		}
	} else {
		c.Database.Url = url
	}

}

func (c *Mqtt2db) createEntry(x map[string]interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	tlog.Log.Debugf("Create mapping entry by %#v", x)
	for _, e := range c.Mapping {
		tlog.Log.Debugf("From source %s", e.Source)
		mNames := strings.Split(e.Source, "/")
		var i interface{}
		i = x
		for _, s := range mNames {
			tlog.Log.Debugf("Take %s", s)
			if subMap, ok := i.(map[string]interface{})[s]; ok {
				i = subMap
			} else {
				break
			}
		}
		tlog.Log.Debugf("Destination %s %v", e.Destination, i)
		// t := reflect.TypeOf(e.Type)
		f := reflectType(e.Type, i)

		m[e.Destination] = f
		tlog.Log.Debugf("Type %s -> %T %v", e.Type, f, f)
	}
	return m
}

func reflectType(fdType string, i interface{}) interface{} {
	var t reflect.Type
	switch fdType {
	case "int64":
		t = reflect.TypeOf(int64(0))
	case "float64":
		t = reflect.TypeOf(float64(0))
	case "string":
		t = reflect.TypeOf("")
	case "time.Time":
		t = reflect.TypeOf(time.Now())

	}
	o := reflect.New(t)
	o = o.Elem()
	tlog.Log.Debugf("Resolve %s %v", fdType, i)
	switch fdType {
	case "time.Time":
		tn, err := time.ParseInLocation(layout, i.(string), time.Local)
		if err != nil {
			log.Fatalf("Parse time location %v", err)
		}
		v := reflect.ValueOf(tn)
		o.Set(v)
	// case "float64":
	// 	i64 := i.(int64)
	// 	fl64 := float64(i64)
	// 	v := reflect.ValueOf(fl64)
	// 	o.Set(v)
	case "int64":
		i64 := i.(float64)
		fl64 := int64(i64)
		v := reflect.ValueOf(fl64)
		o.Set(v)
	default:
		v := reflect.ValueOf(i)
		o.Set(v)
	}
	return o.Interface()
}

func ParseMessage(x map[string]interface{}) map[string]interface{} {
	em := c.createEntry(x)
	if em != nil {
		tlog.Log.Debugf("Return dynamic %v", em)
		counter++
		return em
	}
	services.ServerMessage("No dynamic parsing mapping")
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
			return nil
		}

		m := o.(map[string]interface{})
		if o, ok = m["Power"]; !ok {
			fmt.Println("Error search 'Power'")
			return nil
		}
		em["PowerCurr"] = int64(o.(float64))
		e.PowerCurr = int64(o.(float64))
		if o, ok = m["E_in"]; !ok {
			fmt.Println("Error search 'E_in'")
			return nil
		}
		e.Total = o.(float64)
		em["Total"] = o.(float64)
		if o, ok = m["E_out"]; !ok {
			fmt.Println("Error search 'E_in'")
			return nil
		}
		e.PowerOut = o.(float64)
		em["PowerOut"] = o.(float64)
		if e.PowerCurr < 0 && e.PowerOut == 0 {
			e.PowerOut = float64(-e.PowerCurr)
			e.PowerCurr = 0
			em["PowerOut"] = float64(-em["PowerCurr"].(int64))
			em["PowerCurr"] = 0
		}
		return em
	}
	return nil
}
