package net.spring_boot.rabbitmq.models;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "processing_jobs")
@Getter
@Setter
public class Job {

    @Id
    @GeneratedValue
    @Column(columnDefinition = "uuid")
    private UUID id;

    @Column(name = "file_id", columnDefinition = "uuid", nullable = false)
    private UUID fileId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private JobType type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private JobStatus status;

    @Column(nullable = false)
    private int attempts = 0;

    @Column(nullable = false)
    private int maxAttempts = 3;

    @Column(columnDefinition = "text")
    private String errorMessage;

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(nullable = false)
    private Instant updatedAt = Instant.now();

    private Instant startedAt;

    private Instant completedAt;

    @OneToMany(mappedBy = "job", cascade = jakarta.persistence.CascadeType.ALL, orphanRemoval = true)
    @OrderBy("createdAt ASC")
    private List<JobHistory> history = new ArrayList<>();
}
