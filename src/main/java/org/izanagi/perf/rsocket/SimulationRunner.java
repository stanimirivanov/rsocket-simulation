package org.izanagi.perf.rsocket;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;

@Component
public class SimulationRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(SimulationRunner.class);

    private final RSocketClient client;
    private final SimulationProperties properties;
    private final ConfigurableApplicationContext context;
    private final Sinks.One<Duration> completion = Sinks.one();

    public SimulationRunner(RSocketClient client, SimulationProperties properties,
                            ConfigurableApplicationContext context) {
        this.client = client;
        this.properties = properties;
        this.context = context;
    }

    @Override
    public void run(@NonNull ApplicationArguments args) {
        Instant startedAt = Instant.now();
        Duration delay = properties.rampUp().dividedBy(Math.max(1, properties.connections()));
        log.info("Starting shard {} with {} connections for {}", properties.shardIndex(),
                properties.connections(), properties.duration());

        Flux.range(0, properties.connections())
                .delayElements(delay)
                .flatMap(client::run, properties.connections())
                .then()
                .doOnSuccess(ignored -> {
                    Duration elapsed = Duration.between(startedAt, Instant.now());
                    log.info("Shard {} completed after {}; received {} messages",
                            properties.shardIndex(), elapsed, client.messageCount());
                    completion.tryEmitValue(elapsed);
                })
                .doOnError(completion::tryEmitError)
                .doFinally(ignored -> {
                    if (properties.exitOnCompletion()) {
                        context.close();
                    }
                })
                .subscribe();
    }

    public Mono<Duration> completion() {
        return completion.asMono();
    }
}
