FROM golang:1.14-alpine as build
COPY . /s5cmd/
RUN cd /s5cmd/ && \
    go build -mod=vendor -o s5cmd main.go

FROM alpine:3.12
COPY --from=build /s5cmd/s5cmd .
ENTRYPOINT ["./s5cmd"]
