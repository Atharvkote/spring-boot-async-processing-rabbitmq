package net.spring_boot.rabbitmq.service;

import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;
import net.spring_boot.rabbitmq.model.JobMessage;

import java.util.Collection;
import java.util.UUID;

public interface JobService {

    JobMessage createJob(JobType type, int maxAttempts);

    JobMessage findById(UUID id);

    Collection<JobMessage> findAll();

    void save(JobMessage job);
}
