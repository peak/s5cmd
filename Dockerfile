FROM golang:1.14-alpine

RUN \
    apk add --no-cache git && \
    git clone https://github.com/peak/s5cmd && cd s5cmd && \
    go get ./... && \
    go build -o s5cmd main.go