package net.spring_boot.rabbitmq.models;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import net.spring_boot.rabbitmq.enums.JobStatus;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "job_history")
@Getter
@Setter
public class JobHistory {

    @Id
    @GeneratedValue
    @Column(columnDefinition = "uuid")
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_id", nullable = false)
    private Job job;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private JobStatus previousStatus;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private JobStatus newStatus;

    @Column(nullable = false)
    private int attempt;

    @Column(columnDefinition = "text")
    private String message;

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();
}
