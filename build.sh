#!/bin/sh

VERSION=${VERSION:-v1.0.0}

go build -ldflags "-X main.BuildVersion=${VERSION} -X main.BuildDate=$(date +%d-%m-%Y'_'%H:%M:%S)" -o docker/mqtt2db ./cmd/mqtt2db
