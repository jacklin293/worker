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

### Local Queue

1. docker-compose up -d
2. http://localhost:9325/

config:

```json
[{
    "name":"queue-1",
    "source_type":"sqs",
    "endpoint":"http://localhost:9324/",
    "topic":"queue/default",
    "concurrency":3,
    "enabled":true,
    "metadata":{
        "sqs":{
            "use_local_sqs": true,
            "region":"us-east-1"
        }
    }
}]
```
