package net.spring_boot.rabbitmq.service;

import net.spring_boot.rabbitmq.dto.JobHistoryResponse;
import net.spring_boot.rabbitmq.models.Job;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;

import java.util.List;
import java.util.UUID;

public interface JobService {

    Job createJob(JobType type, int maxAttempts);

    Job findById(UUID id);

    List<Job> findAll();

    Job save(Job job);

    void recordHistory(Job job, JobStatus previousStatus, JobStatus newStatus, int attempt, String message);

    List<JobHistoryResponse> getHistory(UUID jobId);
}
