package net.spring_boot.rabbitmq.controller;

import net.spring_boot.rabbitmq.dtos.jobs.CreateJobRequest;
import net.spring_boot.rabbitmq.exception.ResourceNotFoundException;
import net.spring_boot.rabbitmq.model.JobMessage;
import net.spring_boot.rabbitmq.responses.Response;
import net.spring_boot.rabbitmq.queues.JobProducer;
import net.spring_boot.rabbitmq.service.JobService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;
    private final JobProducer jobProducer;

    public JobController(JobService jobService, JobProducer jobProducer) {
        this.jobService = jobService;
        this.jobProducer = jobProducer;
    }

    @PostMapping
    public ResponseEntity<Response> createJob(@RequestBody CreateJobRequest request) {
        JobMessage job = jobService.createJob(request.getType(), request.getMaxAttempts());
        jobProducer.publishJob(job);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Response.success("Job created and queued", job));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Response> getJob(@PathVariable UUID id) {
        JobMessage job = jobService.findById(id);
        if (job == null) {
            throw new ResourceNotFoundException("Job not found: " + id);
        }
        return ResponseEntity.ok(Response.success(job));
    }

    @GetMapping
    public ResponseEntity<Response> getAllJobs() {
        Collection<JobMessage> jobs = jobService.findAll();
        return ResponseEntity.ok(Response.success(jobs));
    }
}
