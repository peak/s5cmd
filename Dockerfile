FROM golang:1.14-alpine as build
COPY . /s5cmd/
RUN apk add --no-cache git make && \
    cd /s5cmd/ && \
    make build

FROM alpine:3.12
COPY --from=build /s5cmd/s5cmd .
ENTRYPOINT ["./s5cmd"]
