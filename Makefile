BINS=s5cmd

include go.mk

s5cmd:  ${SRCDIR}/*.go
	${GOROOT}/bin/go build ${GCFLAGS} -ldflags "${LDFLAGS}" ./$(<D)
