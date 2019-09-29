# more pprof https://gist.github.com/arsham/bbc93990d8e5c9b54128a3d88901ab90

quick:
	go test -v -tags=integration -run TestBasicJob
unit:
	go test -v -tags=unit -cover
	go test -v -tags=unit queue/* -cover
integration:
	go test -v -tags=integration -cover
	go test -v -tags=integration queue/* -cover
cover:
	go test -v -tags=unit ./... -coverprofile=unit-coverage.out
	go tool cover -html=unit-coverage.out
	go test -v -tags=integration ./... -coverprofile=integration-coverage.out
	go tool cover -html=integration-coverage.out
bench:
	go test -v -tags=bench -bench=. -run=^a -benchmem
vbench:
	go test -v -tags=integration -bench=. -run=^a -benchmem -cpuprofile=cpu.out -memprofile=mem.out -trace=trace.out
pprof:
	go tool pprof -pdf $FILENAME.test cpu.out > cpu.pdf && open cpu.pdf
	go tool pprof -pdf --alloc_space $FILENAME.test mem.out > alloc_space.pdf && open alloc_space.pdf
	go tool pprof -pdf --alloc_objects $FILENAME.test mem.out > alloc_objects.pdf && open alloc_objects.pdf
	go tool pprof -pdf --inuse_space $FILENAME.test mem.out > inuse_space.pdf && open inuse_space.pdf
	go tool pprof -pdf --inuse_objects $FILENAME.test mem.out > inuse_objects.pdf && open inuse_objects.pdf
torch:
	go-torch tmp.test cpu.out -f ${FILENAME}_cpu.svg && open -a "Google Chrome" ${FILENAME}_cpu.svg
	go-torch --alloc_objects $FILENAME.test mem.out -f ${FILENAME}_alloc_obj.svg && open -a "Google Chrome" ${FILENAME}_alloc_obj.svg
	go-torch --alloc_space $FILENAME.test mem.out -f ${FILENAME}_alloc_space.svg && open -a "Google Chrome" ${FILENAME}_alloc_space.svg
	go-torch --inuse_objects $FILENAME.test mem.out -f ${FILENAME}_inuse_obj.svg && open -a "Google Chrome" ${FILENAME}_inuse_obj.svg
	go-torch --inuse_space $FILENAME.test mem.out -f ${FILENAME}_inuse_space.svg && open -a "Google Chrome" ${FILENAME}_inuse_space.svg
trace:
	go tool trace trace.out
lint:
	golangci-lint run
clean:
	rm *.out *.svg *.pdf worker.test
build:
	docker-compose up -d
