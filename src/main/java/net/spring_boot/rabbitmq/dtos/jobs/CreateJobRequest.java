package net.spring_boot.rabbitmq.dtos.jobs;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.spring_boot.rabbitmq.enums.JobType;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateJobRequest {
    @NotNull(message = "Document ID is required")
    private UUID documentId;

    @NotNull(message = "Job type is required")
    private JobType type;

    @Min(value = 1, message = "Max attempts must be at least 1")
    @Max(value = 10, message = "Max attempts cannot exceed 10")
    private Integer maxAttempts = 3;
}
