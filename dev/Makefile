.PHONY: dev run build senddev sendprod

dev:
	rm ./pixels; go build ; ./pixels;

run:
	./pixels

build:
	rm -v bin/pixels-linux-amd64; \
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/pixels-linux-amd64

sendprod:
		ssh -i ~/.ssh/mobilink_centos.pem bin/pixels-linux-amd64 centos@52.66.23.201 rm /home/centos/go/src/github.com/vostrok/pixels/bin/pixels-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem bin/pixels-linux-amd64 centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/pixels/bin/pixels-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem dev/appconfig.prod.yml centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/pixels/dev/

senddev:	
	# ssh -i ~/.ssh/mobilink_centos_testing_stage.pem bin/pixels-linux-amd64 centos@35.154.8.158 rm /home/centos/go/src/github.com/vostrok/pixels/bin/pixels-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem bin/pixels-linux-amd64 centos@35.154.8.158:/home/centos/go/src/github.com/vostrok/pixels/bin/pixels-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem dev/appconfig.yml centos@35.154.8.158:/home/centos/go/src/github.com/vostrok/pixels/dev/

