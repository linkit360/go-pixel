.PHONY: rm build

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

run:
	./pixels

rm:
	 rm -v bin/pixels-linux-amd64; rm ~/linkit/pixels-linux-amd64;


build:
	export GOOS=linux; export GOARCH=amd64; \
	sed -i "s/%VERSION%/$(VERSION)/g" /home/centos/vostrok/utils/metrics/metrics.go; \
  go build -ldflags "-s -w" -o bin/pixels-linux-amd64; cp bin/pixels-linux-amd64 ~/linkit/ ; cp dev/pixels.yml ~/linkit/;


all:
	curl "http://localhost:50308/api?limit=10&hours=0"

