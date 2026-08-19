package net.spring_boot.rabbitmq.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "job")
public class JobProperties {

    private Processing processing = new Processing();
    private Retry retry = new Retry();

    @Setter
    @Getter
    public static class Processing {
        private long delayMs = 3000;
        private double failureRate = 0.3;

    }

    @Getter
    @Setter
    public static class Retry {
        private List<Long> delays = List.of(5000L, 15000L, 45000L);
    }
}
