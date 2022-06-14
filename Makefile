build:
	export CGO_LDFLAGS_ALLOW="-Wl,-z,now" && \
	go mod tidy && \
	go build -tags libsqlite3 -o pdns-dqlite cmd/main.go