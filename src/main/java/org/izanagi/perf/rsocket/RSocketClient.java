package org.izanagi.perf.rsocket;

import io.rsocket.RSocket;
import org.izanagi.perf.rsocket.data.SimulationPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RSocketClient {
    private static final Logger log = LoggerFactory.getLogger(RSocketClient.class);

    private final RSocketRequester.Builder requesterBuilder;
    private final SimulationProperties properties;
    private final AtomicLong messages = new AtomicLong();

    public RSocketClient(RSocketRequester.Builder requesterBuilder, SimulationProperties properties) {
        this.requesterBuilder = requesterBuilder;
        this.properties = properties;
    }

    public Mono<Void> run(int clientIndex) {
        String clientId = properties.shardIndex() + "-" + clientIndex;
        return Mono.defer(() -> connect(clientId))
                .flatMap(requester -> holdConnection(requester, clientId))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, properties.reconnectDelay())
                        .doBeforeRetry(signal -> log.warn("Reconnecting client {}: {}", clientId,
                                signal.failure().getMessage())))
                .repeat()
                .take(properties.duration())
                .then();
    }

    private Mono<RSocketRequester> connect(String clientId) {
        return requesterBuilder
                .rsocketConnector(connector -> connector.keepAlive(
                        properties.keepAliveInterval(), properties.keepAliveLifetime()))
                .websocket(properties.serverUri())
                .doOnNext(ignored -> log.debug("Connected client {}", clientId));
    }

    private Mono<Void> holdConnection(RSocketRequester requester, String clientId) {
        SimulationPayload payload = new SimulationPayload(clientId, properties.shardIndex(),
                properties.payloadMessage());
        RSocket rsocket = requester.rsocket();
        Mono<Void> stream = requester.route(properties.route())
                .data(payload)
                .retrieveFlux(Object.class)
                .doOnNext(ignored -> messages.incrementAndGet())
                .then();
        return Mono.firstWithSignal(stream, rsocket.onClose())
                .doFinally(ignored -> rsocket.dispose());
    }

    public long messageCount() {
        return messages.get();
    }
}
