.PHONY: rm build

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

run:
	./pixel

rm:
	 rm -v bin/pixel-linux-amd64; rm ~/linkit/pixel-linux-amd64;


build:
	export GOOS=linux; export GOARCH=amd64; \
	sed -i "s/%VERSION%/$(VERSION)/g" /home/centos/vostrok/utils/metrics/metrics.go; \
  go build -ldflags "-s -w" -o bin/pixel-linux-amd64;


cp:
	 cp bin/pixel-linux-amd64 ~/linkit/ ; cp dev/pixel.yml ~/linkit/;

all:
	curl "http://localhost:50308/api?limit=10&hours=0"

