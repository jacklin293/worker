# Introduction

TODO

# Running testing

    make unit
    make integration

# Contribute

Build development environment

    make build


# Benchmark

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
