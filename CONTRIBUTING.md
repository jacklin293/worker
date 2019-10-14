## Command

Run testing

    make unit
    make integration

Coding convention checking

    make lint

Run benchmark

    make bench

Build development environment

    make build-dev

## pprof

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
