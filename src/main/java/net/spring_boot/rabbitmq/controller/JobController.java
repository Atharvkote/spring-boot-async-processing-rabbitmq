package net.spring_boot.rabbitmq.controller;

import jakarta.validation.Valid;
import net.spring_boot.rabbitmq.dto.CreateJobRequest;
import net.spring_boot.rabbitmq.dto.JobHistoryResponse;
import net.spring_boot.rabbitmq.dto.JobResponse;
import net.spring_boot.rabbitmq.models.Job;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.exception.BadRequestException;
import net.spring_boot.rabbitmq.queues.message.JobMessage;
import net.spring_boot.rabbitmq.queues.producer.JobProducer;
import net.spring_boot.rabbitmq.responses.Response;
import net.spring_boot.rabbitmq.service.JobService;
import net.spring_boot.rabbitmq.service.helpers.JobStateManger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;
    private final JobStateManger jobStateManger;
    private final JobProducer jobProducer;

    public JobController(JobService jobService, JobStateManger jobStateManger, JobProducer jobProducer) {
        this.jobService = jobService;
        this.jobStateManger = jobStateManger;
        this.jobProducer = jobProducer;
    }

    @PostMapping
    public ResponseEntity<Response> createJob(@Valid @RequestBody CreateJobRequest request) {
        Job job = jobService.createJob(request.getType(), request.getMaxAttempts());

        JobMessage jobMessage = new JobMessage(
                job.getId(),
                job.getType(),
                1,
                job.getMaxAttempts(),
                job.getCreatedAt()
        );
        jobProducer.publishJob(jobMessage);

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Response.success("Job created and queued", toResponse(job)));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Response> getJob(@PathVariable UUID id) {
        Job job = jobService.findById(id);
        return ResponseEntity.ok(Response.success(toResponse(job)));
    }

    @GetMapping
    public ResponseEntity<Response> getAllJobs() {
        List<Job> jobs = jobService.findAll();
        List<JobResponse> responses = jobs.stream()
                .map(this::toResponse)
                .collect(Collectors.toList());
        return ResponseEntity.ok(Response.success(responses));
    }

    @GetMapping("/{id}/history")
    public ResponseEntity<Response> getJobHistory(@PathVariable UUID id) {
        jobService.findById(id);
        List<JobHistoryResponse> history = jobService.getHistory(id);
        return ResponseEntity.ok(Response.success(history));
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<Response> retryJob(@PathVariable UUID id) {
        Job job = jobService.findById(id);

        if (job.getStatus() != JobStatus.FAILED && job.getStatus() != JobStatus.DEAD_LETTERED) {
            throw new BadRequestException("Job cannot be retried from state: " + job.getStatus());
        }

        jobStateManger.validateTransition(job.getStatus(), JobStatus.QUEUED);
        job.setStatus(JobStatus.QUEUED);
        job.setAttempts(0);
        job.setErrorMessage(null);
        jobService.save(job);
        jobService.recordHistory(job, job.getStatus(), JobStatus.QUEUED, 0, "Manual retry initiated");

        JobMessage jobMessage = new JobMessage(
                job.getId(),
                job.getType(),
                1,
                job.getMaxAttempts(),
                job.getCreatedAt()
        );
        jobProducer.publishJob(jobMessage);

        return ResponseEntity.ok(Response.success("Job queued for retry", toResponse(job)));
    }

    private JobResponse toResponse(Job job) {
        return new JobResponse(
                job.getId(),
                job.getType(),
                job.getStatus(),
                job.getAttempts(),
                job.getMaxAttempts(),
                job.getErrorMessage(),
                job.getCreatedAt(),
                job.getStartedAt(),
                job.getCompletedAt()
        );
    }
}
