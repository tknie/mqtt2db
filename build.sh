#!/bin/sh

VERSION=${VERSION:-v1.0.0}
PACKAGE=github.com/tknie/mqtt2db

go build -ldflags "-X ${PACKAGE}.BuildVersion=${VERSION} -X ${PACKAGE}.BuildDate=$(date +%d-%m-%Y'_'%H:%M:%S)" -o docker/mqtt2db ./cmd/mqtt2db
