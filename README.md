# RSocket connection simulator

A generic, finite RSocket request-stream load generator. It opens many WebSocket
RSocket connections, subscribes each connection to a route, keeps them alive for
a configured duration, and exits with the Kubernetes Job. Multiple pods divide a
large test into independent shards.

## Architecture

An **Indexed Kubernetes Job** is the right primitive for this finite workload.
Each pod receives a stable
`JOB_COMPLETION_INDEX` and opens `connectionsPerShard` connections. For example,
8 shards × 500 connections requests 4,000 total connections. `parallelism` can
be lower than `shards` when cluster capacity requires waves of pods.

There is no general 4,000-connection container limit. Practical capacity depends
on file-descriptor limits, heap/direct memory, TLS, event-loop and CPU capacity,
message rate, and node/network limits. Start with a small per-shard value,
observe pod and server metrics, then increase it. Sharding makes the load
portable and prevents one pod or node from becoming the only bottleneck.

## Build and run locally

Prerequisites: JDK 25 and Maven 3.6.3 or newer.

```shell
mvn clean verify
SIMULATION_SERVER_URI=ws://localhost:7000/rsocket \
SIMULATION_CONNECTIONS=100 \
SIMULATION_DURATION=PT5M \
java -jar target/rsocket-simulation-2.0.0-SNAPSHOT.jar
```

All settings use Spring Boot externalized configuration. See
`src/main/resources/application.yml` for the environment variables and defaults.
Durations use ISO-8601 values such as `PT30S` and `PT10M`.

The generic payload is:

```json
{
  "clientId": "3-42",
  "shardIndex": 3,
  "message": "hello from the RSocket simulator"
}
```

The simulator currently exercises request-stream. Authentication is
intentionally not target-specific; when required, add generic setup metadata and
inject credentials through a Kubernetes Secret.

## Container image

The Fabric8 Docker Maven Plugin builds an image using the packaged Spring Boot
jar:

```shell
mvn clean package docker:build \
  -Dimage.name=registry.example.com/performance/rsocket-simulation \
  -Dimage.tag=2.0.0
mvn docker:push \
  -Dimage.name=registry.example.com/performance/rsocket-simulation \
  -Dimage.tag=2.0.0
```

The Docker daemon and registry credentials must be available to Maven.

## Helm

Configuration belongs in a versioned values file for repeatable tests. Use
`--set` only for run-specific overrides:

```shell
helm lint charts/rsocket-simulation
helm upgrade --install demo charts/rsocket-simulation \
  -f my-test-values.yaml \
  --set-string image.repository=registry.example.com/performance/rsocket-simulation \
  --set-string image.tag=2.0.0 \
  --set-string simulation.serverUri=wss://server.example.com/rsocket
```

The chart renders non-secret configuration into a ConfigMap. Do not put tokens
in Helm values or
`--set`; use a separately managed Secret when generic authentication support is
added.

## Tekton execution

Tekton owns the simulation lifecycle: it clones the selected revision, lints the
chart, launches the Indexed Job, waits for completion, and prints a bounded log
tail.

```shell
kubectl apply -f tekton/rbac.yaml
kubectl apply -f tekton/tasks.yaml
kubectl apply -f tekton/pipeline.yaml
kubectl create -f tekton/pipelinerun.example.yaml
tkn pipelinerun logs --last -f
```

Copy the example PipelineRun and set a unique `releaseName` per run. The service
account is namespace scoped. In a shared cluster, install these resources in a
dedicated load-test namespace and apply ResourceQuota.
