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

import tlog "github.com/tknie/log"

type MQTTWrapperLogger struct {
}

func (l *MQTTWrapperLogger) Println(v ...interface{}) {
	s := ""
	for range v {
		s += "%v "
	}
	s += "\n"
	tlog.Log.Debugf(s, v...)
}

func (l *MQTTWrapperLogger) Printf(format string, v ...interface{}) {
	tlog.Log.Debugf(format, v...)
}
