BINS=s5cmd

include go.mk

s5cmd:  ${SRCDIR}/*.go ${SRCDIR}/core/*.go ${SRCDIR}/op/*.go ${SRCDIR}/opt/*.go ${SRCDIR}/stats/*.go ${SRCDIR}/version/*.go
	${GOROOT}/bin/go build ${GCFLAGS} -ldflags "${LDFLAGS}" ./$(<D)
