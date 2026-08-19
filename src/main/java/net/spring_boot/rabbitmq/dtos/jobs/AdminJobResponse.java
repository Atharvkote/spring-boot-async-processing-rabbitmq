package net.spring_boot.rabbitmq.dtos.jobs;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdminJobResponse {
    private UUID id;
    private UUID documentId;
    private UUID userId;
    private JobType type;
    private JobStatus status;
    private int attempts;
    private int maxAttempts;
    private String errorMessage;
    private Instant createdAt;
    private Instant startedAt;
    private Instant completedAt;
}
