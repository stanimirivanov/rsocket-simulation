package org.izanagi.perf.rsocket;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;

@Validated
@ConfigurationProperties("simulation")
public record SimulationProperties(
        @NotNull URI serverUri,
        @NotBlank String route,
        @Min(1) int connections,
        @NotNull Duration duration,
        @NotNull Duration rampUp,
        @NotNull Duration keepAliveInterval,
        @NotNull Duration keepAliveLifetime,
        @NotNull Duration reconnectDelay,
        @Min(0) int shardIndex,
        @NotBlank String payloadMessage) {
}
