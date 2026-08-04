package org.izanagi.perf.rsocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Component
public class SimulationRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(SimulationRunner.class);

    private final RSocketClient client;
    private final SimulationProperties properties;
    private final ConfigurableApplicationContext context;

    public SimulationRunner(RSocketClient client, SimulationProperties properties,
                            ConfigurableApplicationContext context) {
        this.client = client;
        this.properties = properties;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        Duration delay = properties.rampUp().dividedBy(Math.max(1, properties.connections()));
        log.info("Starting shard {} with {} connections for {}", properties.shardIndex(),
                properties.connections(), properties.duration());

        Flux.range(0, properties.connections())
                .delayElements(delay)
                .flatMap(client::run, properties.connections())
                .then()
                .doOnSuccess(ignored -> log.info("Shard {} completed; received {} messages",
                        properties.shardIndex(), client.messageCount()))
                .doFinally(ignored -> context.close())
                .subscribe();
    }
}
