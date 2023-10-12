FROM golang:1.20-alpine as build
COPY . /s5cmd/
RUN apk add --no-cache git make && \
    cd /s5cmd/ && \
    CGO_ENABLED=0 make build

FROM alpine:3.18
COPY --from=build /s5cmd/s5cmd .
WORKDIR /aws
ENTRYPOINT ["/s5cmd"]
