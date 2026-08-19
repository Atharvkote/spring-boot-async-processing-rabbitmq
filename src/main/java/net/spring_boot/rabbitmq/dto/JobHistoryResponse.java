package net.spring_boot.rabbitmq.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobHistoryResponse {
    private UUID id;
    private UUID jobId;
    private String previousStatus;
    private String newStatus;
    private String message;
    private Instant createdAt;
}
