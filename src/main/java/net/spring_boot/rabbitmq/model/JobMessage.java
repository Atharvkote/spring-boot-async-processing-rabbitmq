package net.spring_boot.rabbitmq.model;

import lombok.Getter;
import lombok.Setter;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;
import java.time.Instant;
import java.util.UUID;

@Setter
@Getter
public class JobMessage {
    private UUID id;
    private JobType type;
    private JobStatus status;
    private int attempts;
    private int maxAttempts;
    private String errorMessage;
    private Instant createdAt;
    private Instant startedAt;
    private Instant completedAt;

    public JobMessage() {}

    public JobMessage(UUID id, JobType type, JobStatus status, int attempts, int maxAttempts) {
        this.id = id;
        this.type = type;
        this.status = status;
        this.attempts = attempts;
        this.maxAttempts = maxAttempts;
        this.createdAt = Instant.now();
    }

}
