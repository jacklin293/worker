# TODO


# Benchmark

    go test -v -tags=bench -bench=. -run=^a -benchmem
    goos: darwin
    goarch: amd64
    pkg: worker
    BenchmarkBasic-4          500000              2948 ns/op             464 B/op         10 allocs/op
    --- BENCH: BenchmarkBasic-4
        bench_test.go:45: counter/total: 1/1
        bench_test.go:45: counter/total: 10/10
        bench_test.go:45: counter/total: 200/200
        bench_test.go:45: counter/total: 3000/3000
        bench_test.go:45: counter/total: 50000/50000
        bench_test.go:45: counter/total: 300000/300000
        bench_test.go:45: counter/total: 500000/500000
    PASS
    ok      worker  3.167s

# Contribute

### Run testing

    make unit
    make integration

### Coding convention checking

    make lint


### Run benchmark

    make bench

### Build development environment

    make build-dev

### pprof

Install

    brew install graphviz

Command

    make bench
    make pprof

### go-torch

Insteall

    go get -u github.com/google/pprof

project directory

    git clone https://github.com/brendangregg/FlameGraph

Command

    make bench
    make torch
