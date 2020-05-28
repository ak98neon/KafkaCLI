build:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bin/main .

run:
	go run bin/main

docker:
	docker build -t kafka_cli .
	docker tag kafka_cli akudria/kafka_cli:latest
	docker push akudria/kafka_cli:latest
