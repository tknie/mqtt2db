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
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	tlog "github.com/tknie/log"
	"github.com/tknie/services"
)

var logRus = logrus.StandardLogger()

func StartLog() {
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
