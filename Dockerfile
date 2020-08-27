FROM golang:1.14-alpine
WORKDIR $GOPATH/src/s5cmd/
COPY . .
RUN go build -o s5cmd main.go
CMD ["./s5cmd"]
