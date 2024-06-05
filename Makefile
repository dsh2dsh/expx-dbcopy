PROGNAME=	dbcopy
TEST_ARGS=

build:
	go build -ldflags="-s -w" -o "${PROGNAME}" ./

test:
	go test ${TEST_ARGS} ./...

test-e2e:
	go test ${TEST_ARGS} -tags=e2e ./...

clean:
	rm -f "${PROGNAME}"

${PROGNAME}: build
