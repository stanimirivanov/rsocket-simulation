package org.izanagi.perf.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.util.DefaultPayload;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.main.web-application-type=none",
        "simulation.route=simulation.stream",
        "simulation.connections=2",
        "simulation.duration=PT3S",
        "simulation.ramp-up=PT0S",
        "simulation.keep-alive-interval=PT0.1S",
        "simulation.keep-alive-lifetime=PT1S",
        "simulation.reconnect-delay=PT0.1S",
        "simulation.shard-index=7",
        "simulation.payload-message=integration test",
        "simulation.exit-on-completion=false"
})
class RSocketClientApplicationIntegrationTest {

    private static final AtomicInteger SUBSCRIPTIONS = new AtomicInteger();
    private static final CloseableChannel SERVER = startServer();

    @Autowired
    private SimulationRunner simulationRunner;

    @Autowired
    private RSocketClient rSocketClient;

    private static CloseableChannel startServer() {
        RSocket responder = new RSocket() {
            @Override
            public @NonNull Flux<Payload> requestStream(Payload payload) {
                SUBSCRIPTIONS.incrementAndGet();
                payload.release();
                return Flux.just(DefaultPayload.create("{\"message\":\"hello from test server\"}"))
                        .delayElements(Duration.ofMillis(100));
            }
        };

        return requireNonNull(RSocketServer.create((setup, sendingSocket) ->
                        reactor.core.publisher.Mono.just(responder))
                .bind(TcpServerTransport.create("127.0.0.1", 0))
                .block(Duration.ofSeconds(5)));
    }

    @DynamicPropertySource
    static void serverUri(DynamicPropertyRegistry registry) {
        registry.add("simulation.server-uri",
                () -> "tcp://127.0.0.1:" + ((InetSocketAddress) SERVER.address()).getPort());
    }

    @AfterAll
    static void stopServer() {
        SERVER.dispose();
    }

    @Test
    void applicationRunsForConfiguredDurationAndOpensConfiguredConnections() {
        StepVerifier.create(simulationRunner.completion()
                        .timeout(Duration.ofSeconds(10)))
                .expectNextMatches(elapsed -> elapsed.compareTo(Duration.ofSeconds(3)) >= 0)
                .verifyComplete();

        assertThat(SUBSCRIPTIONS).hasValueGreaterThanOrEqualTo(2);
        assertThat(rSocketClient.messageCount()).isGreaterThanOrEqualTo(2);
    }
}
