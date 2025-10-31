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
	"strings"
	"time"

	"github.com/tknie/flynn"
	"github.com/tknie/flynn/common"
	tlog "github.com/tknie/log"
	"github.com/tknie/services"
)

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

var dbid common.RegDbID

type data struct {
	timeRow     time.Time
	insertedRow time.Time
	id          int32
}

func (d *data) String() string {
	return fmt.Sprintf("[%d:%v/%v]", d.id, d.timeRow.UTC().Format(layout), d.insertedRow.UTC().Format(layout))
}

// InitDatabase initialize database by
//   - creating storage table
//   - create index for inserted_on
//   - create function for updating inserted_on the current
//     timestamp
//   - add id serial
func InitDatabase(create bool, tries int) {

	dbRef, password := getUrl()
	if c.Database.StoreTablename == "" {
		services.ServerMessage("Database table not defined to store MQTT data")
		os.Exit(10)
	}

	services.ServerMessage("Storage of MQTT data to table '%s'", c.Database.StoreTablename)
	id, err := flynn.Handler(dbRef, password)
	if err != nil {
		services.ServerMessage("Register error log: %v", err)
		log.Fatalf("Register error log: %v", err)
	}
	var status common.CreateStatus
	count := 0
	for count < tries {
		count++
		tlog.Log.Debugf("Try count=%d", count)

		if create {
			// create table if not exists
			status, err = id.CreateTableIfNotExists(c.Database.StoreTablename, &event{})
			if err != nil {
				if count < 10 {
					services.ServerMessage("Wait because of creation err %T: %v", status, err)
					time.Sleep(10 * time.Second)
					services.ServerMessage("Skip counter increased to %d", count)
					continue
				} else {
					services.ServerMessage("Database storage creating failed: %v %T", err, status)
					log.Fatalf("Database storage creating failed: %v %T", err, status)
				}
			}
			tlog.Log.Debugf("Received status=%v", status)
			// if database is created, then call batch commands
			if status == common.CreateCreated {
				for i, batch := range SQLbatches {
					b := strings.Replace(batch, "public.home", "public."+c.Database.StoreTablename, -1)
					err = id.Batch(b)
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
			services.ServerMessage("Database pinging successfullly done")
			services.ServerMessage("Database initiated")
			return
		}
		tlog.Log.Debugf("End error=%v", err)
	}

}

// close close and unregister flynn identifier
func Close() {
	defer dbid.FreeHandler()
}

func storeEvent(e map[string]interface{}) {
	list := [][]any{{e}}
	keys := make([]string, 0, len(e))
	for k := range e {
		keys = append(keys, k)
	}
	insert := &common.Entries{Fields: keys,
		Update: keys,
		Values: list}
	_, err := dbid.Insert(c.Database.StoreTablename, insert)
	if err != nil {
		log.Fatal("Error inserting record: ", err)
	}

}

func SyncDatabase() {
	dbRef, password := getUrl()

	services.ServerMessage("Synchronize of Home data")
	sid, err := flynn.Handler(dbRef, password)
	if err != nil {
		services.ServerMessage("Register error log: %v", err)
		log.Fatalf("Register error log: %v", err)
	}

	url := os.Getenv("MQTT_DEST_URL")
	if url == "" {
		log.Fatal("Destination Table MQTT_DEST_URL parameter not defined...")
	}
	dbRef, password, err = common.NewReference(url)
	if err != nil {
		log.Fatal("REST audit URL incorrect: " + url)
	}
	if password == "" {
		password = os.Getenv("MQTT_DEST_PASS")
	}
	did, err := flynn.Handler(dbRef, password)
	if err != nil {
		services.ServerMessage("Register error log: %v", err)
		log.Fatalf("Register error log: %v", err)
	}
	schan := make(chan *data)
	dchan := make(chan *data)
	go query(sid, schan)
	go query(did, dchan)

	stime := <-schan
	dtime := <-dchan
	counter := uint64(0)
	direction := 0
	rowrepeat := 25
	for {
		diff := stime.insertedRow.Sub(dtime.insertedRow)
		// fmt.Println(diff)
		counter++
		switch {
		case diff < time.Duration(1*time.Minute):
			// fmt.Printf("%03d: %v -> %v -> %v\n", counter, stime, diff, dtime)
			stime = <-schan
			dtime = <-dchan
			direction = 0
		case stime.insertedRow.Before(dtime.insertedRow):
			if direction != 1 || rowrepeat < 1 {
				fmt.Printf("%07d: Source: %12v -> %v -> Target: %v\n", counter, stime, diff, dtime)
				direction = 1
				rowrepeat = 25
			} else {
				fmt.Printf("%07d:                 -> %v -> Target: %v\n", counter, diff, dtime)
				rowrepeat--
			}
			dtime = <-dchan
		case stime.insertedRow.After(dtime.insertedRow):
			if direction != 2 || rowrepeat < 1 {
				fmt.Printf("%07d: Source: %12v -> %v -> Target: %v %0000d\n", counter, stime, diff, dtime, direction)
				direction = 2
				rowrepeat = 25
			} else {
				fmt.Printf("%07d: Source: %v -> %v -> \n", counter, dtime, diff)
				rowrepeat--
			}
			stime = <-schan
		}
		if stime == nil && dtime == nil {
			return
		}
		if stime == nil {
			fmt.Println("Source rest")
			for {
				q := <-dchan
				if q == nil {
					break
				}
				fmt.Printf("%03d:                 -> %v -> Target: %v\n", counter, diff, dtime)
				counter++
			}
			fmt.Println("Source ended")
			return
		}
		if dtime == nil {
			fmt.Println("Destination rest")
			for {
				q := <-schan
				if q == nil {
					break
				}
				fmt.Printf("%03d: Source: %v -> %v -> \n", counter, dtime, diff)
				counter++
			}
			fmt.Println("Destination ended")
			return
		}
	}
}

func query(id common.RegDbID, ch chan *data) {
	query := &common.Query{Search: "SELECT inserted_on, time, id from HOME order by time asc"}
	err := id.BatchSelectFct(query, func(search *common.Query, result *common.Result) error {
		ch <- &data{result.Rows[1].(time.Time), result.Rows[0].(time.Time), result.Rows[2].(int32)}
		return nil
	})
	if err != nil {
		log.Fatal("Error query:", err)
	}
	ch <- nil
	fmt.Println("Query ended...")
}
