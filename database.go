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

type Home struct {
	ID          uint64
	Time        time.Time
	Total       float64
	Powercurr   int64
	Powerout    float64
	Inserted_on time.Time
}

func (d *Home) String() string {
	return fmt.Sprintf("[%d:%v/%v]", d.ID, d.Time.UTC().Format(layout), d.Inserted_on.UTC().Format(layout))
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

	storeid, storeerr := flynn.Handler(dbRef, password)
	if storeerr != nil {
		services.ServerMessage("Register error log: %v", storeerr)
		log.Fatalf("Register error log: %v", storeerr)
	}

	url := os.Getenv("MQTT_DEST_URL")
	if url == "" {
		log.Fatal("Destination Table MQTT_DEST_URL parameter not defined...")
	}
	dbRef, password, err = common.NewReference(url)
	if err != nil {
		log.Fatal("Database destintaion URL incorrect: " + url)
	}
	if password == "" {
		password = os.Getenv("MQTT_DEST_PASS")
	}
	did, err := flynn.Handler(dbRef, password)
	if err != nil {
		services.ServerMessage("Register error log: %v", err)
		log.Fatalf("Register error log: %v", err)
	}
	fmt.Println("Source      id:", sid)
	fmt.Println("Destination id:", did)
	schan := make(chan *Home)
	dchan := make(chan *Home)
	go query(sid, schan)
	go query(did, dchan)

	stime := <-schan
	dtime := <-dchan
	counter := uint64(0)
	scounter := uint64(0)
	dcounter := uint64(0)
	direction := 0
	rowrepeat := 25
	laststime := stime
	lastdtime := dtime
	for {
		diff := stime.Time.Sub(dtime.Time)
		// fmt.Println(diff)
		switch {
		case diff < time.Duration(2*time.Minute):
			// fmt.Printf("%03d: %v -> %v -> %v\n", counter, stime, diff, dtime)
			stime = <-schan
			scounter++
			dtime = <-dchan
			dcounter++
			direction = 0
			counter++
		case stime.Time.Before(dtime.Time):
			if direction != 1 || rowrepeat < 1 {
				fmt.Printf("%07d: B S: %12v -> %12v -> T: %v\n", counter, stime.Time.Format(layout), diff, dtime.Time.Format(layout))
				direction = 1
				rowrepeat = 25
			} else {
				fmt.Printf("%07d: B S: %v -> %12v -> \n", counter, stime.Time.Format(layout), diff)
				rowrepeat--
			}
			stime = <-schan
			dcounter++
			counter++
		case stime.Time.After(dtime.Time):
			if direction != 2 || rowrepeat < 1 {
				fmt.Printf("%07d: A S: %12v -> %12v -> T: %v %0000d\n", counter, stime.Time.Format(layout), diff, dtime.Time.Format(layout), direction)
				direction = 2
				rowrepeat = 25
			} else {
				fmt.Printf("%07d: A                        -> %12v -> T: %v\n", counter, diff, dtime.Time.Format(layout))
				rowrepeat--
			}
			storeHome(storeid, dtime)
			dtime = <-dchan
			scounter++
			counter++
		}
		if stime == nil && dtime == nil {
			fmt.Println("Both ended", laststime.Time.Format(layout), lastdtime.Time.Format(layout))
			return
		}
		if stime == nil {
			fmt.Println("Source rest")
			diff = 0
			for {
				q := <-dchan
				if q == nil {
					break
				}
				dcounter++
				fmt.Printf("%07d: D                        -> %v -> Target: %v\n",
					counter, diff, q.Time.Format(layout))
				storeHome(storeid, dtime)
				lastdtime = q
				counter++
			}
			fmt.Println("Source ended", counter, scounter, dcounter, laststime.Time.Format(layout), lastdtime.Time.Format(layout))
			return
		}
		if dtime == nil {
			fmt.Println("Destination rest")
			diff = 0
			for {
				q := <-schan
				if q == nil {
					break
				}
				fmt.Printf("%07d: S: %v -> %v -> \n", counter, q.Time.Format(layout), diff)
				laststime = q
				counter++
				scounter++
			}
			fmt.Println("Destination ended", counter, scounter, dcounter, laststime.Time.Format(layout), lastdtime.Time.Format(layout))
			return
		}
		laststime = stime
		lastdtime = dtime
	}
}

func query(id common.RegDbID, ch chan *Home) {
	query := &common.Query{
		TableName:  c.Database.StoreTablename,
		DataStruct: &Home{},
		Fields:     []string{"*"},
		Order:      []string{"time:ASC"},
	}
	_, err := id.Query(query, func(search *common.Query, result *common.Result) error {
		ch <- result.Data.(*Home)
		return nil
	})
	if err != nil {
		log.Fatalf("Error query %d: %v", id, err)
	}
	ch <- nil
	fmt.Println(id, "Query ended...")
}

func storeHome(id common.RegDbID, entry *Home) {
	tlog.Log.Debugf("Store Home entry")
	list := [][]any{{entry}}
	keys := []string{"Time", "Total",
		"Powercurr", "Powerout"}
	insert := &common.Entries{Fields: keys,
		DataStruct: entry,
		Update:     keys,
		Values:     list}
	_, err := id.Insert(c.Database.StoreTablename, insert)
	if err != nil {
		log.Fatal("Error inserting record: ", err)
	}

}
