all: build

fmt:
	gofmt -l -w -s src/

test:


build:dep
	go build -o bin/pd src/main.go

clean:
	rm -rf output

dep:fmt
