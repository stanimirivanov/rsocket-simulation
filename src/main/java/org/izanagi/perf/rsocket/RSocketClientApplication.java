package org.izanagi.perf.rsocket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(SimulationProperties.class)
public class RSocketClientApplication {
    public static void main(String[] args) {
        SpringApplication.run(RSocketClientApplication.class, args);
    }
}
