include go.mk

s5cmd:  ${SRCDIR}/cmd/s5cmd/main.go ${SRCDIR}/*.go
	${GOROOT}/bin/go build ${GCFLAGS} -ldflags "${LDFLAGS}" ./$(<D)
