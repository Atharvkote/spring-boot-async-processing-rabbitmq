package net.spring_boot.rabbitmq.service.impls;

import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;
import net.spring_boot.rabbitmq.model.JobMessage;
import net.spring_boot.rabbitmq.service.JobService;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class JobServiceImpl implements JobService {

    private final Map<UUID, JobMessage> jobs = new ConcurrentHashMap<>();

    @Override
    public JobMessage createJob(JobType type, int maxAttempts) {
        UUID id = UUID.randomUUID();
        JobMessage job = new JobMessage(id, type, JobStatus.QUEUED, 0, maxAttempts);
        jobs.put(id, job);
        return job;
    }

    @Override
    public JobMessage findById(UUID id) {
        return jobs.get(id);
    }

    @Override
    public Collection<JobMessage> findAll() {
        return jobs.values();
    }

    @Override
    public void save(JobMessage job) {
        jobs.put(job.getId(), job);
    }
}
