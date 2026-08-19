package net.spring_boot.rabbitmq.service.impls;

import net.spring_boot.rabbitmq.dto.JobHistoryResponse;
import net.spring_boot.rabbitmq.models.Job;
import net.spring_boot.rabbitmq.models.JobHistory;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;
import net.spring_boot.rabbitmq.exception.ResourceNotFoundException;
import net.spring_boot.rabbitmq.repository.JobHistoryRepository;
import net.spring_boot.rabbitmq.repository.JobRepository;
import net.spring_boot.rabbitmq.service.JobService;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class JobServiceImpl implements JobService {

    private final JobRepository jobRepository;
    private final JobHistoryRepository jobHistoryRepository;

    public JobServiceImpl(JobRepository jobRepository, JobHistoryRepository jobHistoryRepository) {
        this.jobRepository = jobRepository;
        this.jobHistoryRepository = jobHistoryRepository;
    }

    @Override
    public Job createJob(JobType type, int maxAttempts) {
        Job job = new Job();
        job.setType(type);
        job.setMaxAttempts(maxAttempts);
        job.setStatus(JobStatus.QUEUED);
        Job saved = jobRepository.save(job);
        recordHistory(saved, null, JobStatus.QUEUED, 0, "Job created");
        return saved;
    }

    @Override
    public Job findById(UUID id) {
        return jobRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Job not found: " + id));
    }

    @Override
    public List<Job> findAll() {
        return jobRepository.findAll();
    }

    @Override
    public Job save(Job job) {
        job.setUpdatedAt(java.time.Instant.now());
        return jobRepository.save(job);
    }

    @Override
    public void recordHistory(Job job, JobStatus previousStatus, JobStatus newStatus, int attempt, String message) {
        JobHistory history = new JobHistory();
        history.setJob(job);
        history.setPreviousStatus(previousStatus);
        history.setNewStatus(newStatus);
        history.setAttempt(attempt);
        history.setMessage(message);
        jobHistoryRepository.save(history);
    }

    @Override
    public List<JobHistoryResponse> getHistory(UUID jobId) {
        return jobHistoryRepository.findAll()
                .stream()
                .filter(h -> h.getJob().getId().equals(jobId))
                .map(h -> new JobHistoryResponse(
                        h.getId(),
                        h.getJob().getId(),
                        h.getPreviousStatus() != null ? h.getPreviousStatus().name() : null,
                        h.getNewStatus().name(),
                        h.getMessage(),
                        h.getCreatedAt()
                ))
                .collect(Collectors.toList());
    }
}
