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
	"log"
	"os"
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
var tableName string

func init() {
	tableName = os.Getenv("MQTT_STORE_TABLENAME")
}

// InitDatabase initialize database by
//   - creating storage table
//   - create index for inserted_on
//   - create function for updating inserted_on the current
//     timestamp
//   - add id serial
func InitDatabase(create bool, tries int) {
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

	services.ServerMessage("Storage of MQTT data to table '%s'", tableName)
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
			status, err = id.CreateTableIfNotExists(tableName, &event{})
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
	_, err := dbid.Insert(tableName, insert)
	if err != nil {
		log.Fatal("Error inserting record: ", err)
	}

}
