build:
	go build -o go-pipes ./cmd/app/

check: fmt errcheck vet

fmt:
	@if [ -n "$$(go fmt ./...)" ]; then echo 'Please run go fmt on your code.' && exit 1; fi

errcheck:
	errcheck -ignoretests -verbose ./...

vet:
	go vet ./...

test:
	go test -cover ./...

coverage:
	overalls -covermode=set -project=github.com/Bigomby/go-pipes
	goveralls -coverprofile=overalls.coverprofile -service=travis-ci

get_dev_deps:
	go get golang.org/x/tools/cmd/cover
	go get golang.org/x/tools/cmd/vet
	go get github.com/kisielk/errcheck
	go get github.com/stretchr/testify/assert
	go get github.com/mattn/goveralls
	go get github.com/axw/gocov/gocov
	go get github.com/go-playground/overalls

get_deps:
	go get -t ./...