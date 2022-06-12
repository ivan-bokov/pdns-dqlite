build:
	export CGO_LDFLAGS_ALLOW="-Wl,-z,now"
	go build -tags libsqlite3 cmd/main.go -o pdns-dqlite