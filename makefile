unit:
	go test -v -tags=unit
integration:
	go test -v -tags=integration
bench:
	go test -v -bench=. -tags=integration
lint:
	golangci-lint run
