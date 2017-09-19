all: build

fmt:
	gofmt -l -w -s src/

test:


build:dep
	go build -o bin/pd src/main.go
	go build -o bin/client src/client.go
	go build -o bin/region_server src/region_server.go

clean:
	rm -rf output

dep:fmt
