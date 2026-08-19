package net.spring_boot.rabbitmq.queues.message;

import net.spring_boot.rabbitmq.enums.JobType;

import java.time.Instant;
import java.util.UUID;

public record JobMessage(
        UUID id,
        UUID fileId,
        JobType type,
        int attempt,
        int maxAttempts,
        Instant createdAt
) {
}
