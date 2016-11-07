.PHONY: dev rm run build senddev sendprod

dev:
	rm ./pixels; go build ; ./pixels;

run:
	./pixels

rm:
	 rm -v bin/pixels-linux-amd64; rm ~/linkit/pixels-linux-amd64;


build:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/pixels-linux-amd64; cp bin/pixels-linux-amd64 ~/linkit/



senddev:
	# ssh -i ~/.ssh/mobilink_centos_testing_stage.pem bin/pixels-linux-amd64 centos@35.154.8.158 rm /home/centos/linkit/pixels-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem bin/pixels-linux-amd64 centos@35.154.8.158:/home/centos/linkit/pixels-linux-amd64
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem dev/pixels.yml centos@35.154.8.158:/home/centos/linkit/

config:
		scp -i ~/.ssh/mobilink_centos_testing_stage.pem dev/pixels.yml centos@35.154.8.158:/home/centos/linkit/

sendprod:
		ssh -i ~/.ssh/mobilink_centos.pem bin/pixels-linux-amd64 centos@52.66.23.201 rm /home/centos/linkit/pixels-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem bin/pixels-linux-amd64 centos@52.66.23.201:/home/centos/linkit/pixels-linux-amd64
		scp -i ~/.ssh/mobilink_centos.pem dev/pixels.prod.yml centos@52.66.23.201:/home/centos/linkit/


