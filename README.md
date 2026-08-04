# RSocket connection simulator

A generic, finite RSocket request-stream load generator. It opens many WebSocket
RSocket connections, subscribes each connection to a route, keeps them alive for
a configured duration, and exits with the Kubernetes Job. Multiple pods divide a
large test into independent shards.

## Architecture

For this finite workload, an **Indexed Kubernetes Job** is the chosen primitive.
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

The Fabric8 Docker Maven Plugin builds the packaged Spring Boot jar into a
non-root OCI image. The `image` profile binds image creation to Maven's
`package` phase:

```shell
mvn clean package -Pimage \
  -Dimage.name=registry.example.com/performance/rsocket-simulation \
  -Dimage.tag=2.0.0 \
  -Dimage.source=https://github.com/stanimirivanov/rsocket-simulation

mvn docker:push \
  -Dimage.name=registry.example.com/performance/rsocket-simulation \
  -Dimage.tag=2.0.0
```

`image.name` is the complete registry/repository name and `image.tag` is kept
separate so CI can publish the same repository under a release or Git SHA tag.
The Docker daemon and registry credentials must be available to Maven; Fabric8
uses the normal Docker credential configuration.

For repeatable release builds, override the base image with an immutable digest
rather than relying on the mutable default tag:

```shell
mvn clean package -Pimage \
  -Dimage.name=registry.example.com/performance/rsocket-simulation \
  -Dimage.tag="$GIT_COMMIT" \
  -Dimage.base="eclipse-temurin:25-jre@sha256:<verified-digest>" \
  -Dimage.source="https://github.com/stanimirivanov/rsocket-simulation"
```

The Maven archive timestamp and OCI `created` label are fixed by
`project.build.outputTimestamp`, making application artifacts stable for the
same source tree. Update that property intentionally for a release. For exact
deployment identity, pass the pushed image digest to Helm rather than a mutable
tag.

### Publishing to GitHub Container Registry

The `Publish container image` GitHub Actions workflow tests the application,
builds the image through the Fabric8 Maven profile, and publishes it to GHCR.
It uses the short-lived repository `GITHUB_TOKEN`; no registry password or
personal access token is required.

Push a version tag to publish a release image. A leading `v` is removed from
the image tag:

```shell
git tag v2.0.0
git push origin v2.0.0
# Publishes ghcr.io/stanimirivanov/rsocket-simulation:2.0.0
```

The workflow can also be started from **Actions → Publish container image → Run
workflow**. Supply `image_tag` to choose a tag, or leave it empty to publish
`sha-<12-character-commit>`.

The workflow grants only `contents: read` and `packages: write`. Repository or
organization policy must permit GitHub Actions to create packages. Package
visibility can be adjusted after the first publication in the repository's
package settings.

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
tkn pipelinerun logs --last -f
```

Copy the example PipelineRun and set a unique `releaseName` per run. The service
account is namespace scoped. In a shared cluster, install these resources in a
dedicated load-test namespace and apply ResourceQuota.
