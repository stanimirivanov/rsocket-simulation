package org.izanagi.perf.rsocket.data;

public record SimulationPayload(String clientId, int shardIndex, String message) {
}
