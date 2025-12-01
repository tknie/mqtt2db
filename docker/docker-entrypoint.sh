#!/bin/sh

LOGFILE=${LOGFILE:-/mqtt2db/log/}
export LOGFILE

/mqtt2db/bin/mqtt2db
