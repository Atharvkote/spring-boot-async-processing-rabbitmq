package net.spring_boot.rabbitmq.queues.impls;

import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;
import net.spring_boot.rabbitmq.model.JobMessage;
import net.spring_boot.rabbitmq.queues.JobConsumer;
import net.spring_boot.rabbitmq.queues.JobProducer;
import net.spring_boot.rabbitmq.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class JobConsumerImpl implements JobConsumer {

    private static final Logger log = LoggerFactory.getLogger(JobConsumerImpl.class);

    private final JobService jobService;
    private final JobProducer jobProducer;
    private final ThreadPoolTaskExecutor jobTaskExecutor;

    public JobConsumerImpl(JobService jobService, JobProducer jobProducer,
                           @Qualifier("jobTaskExecutor") ThreadPoolTaskExecutor jobTaskExecutor) {
        this.jobService = jobService;
        this.jobProducer = jobProducer;
        this.jobTaskExecutor = jobTaskExecutor;
    }

    @RabbitListener(queues = "job.queue")
    public void processJob(JobMessage job) {
        log.info("Received job: {} type: {}", job.getId(), job.getType());
        job.setStatus(JobStatus.PROCESSING);
        job.setStartedAt(Instant.now());
        job.setAttempts(job.getAttempts() + 1);
        jobService.save(job);

        jobTaskExecutor.execute(() -> {
            try {
                process(job);
                job.setStatus(JobStatus.COMPLETED);
                job.setCompletedAt(Instant.now());
                jobService.save(job);
                log.info("Job completed: {}", job.getId());
            } catch (Exception e) {
                log.error("Job failed: {} error: {}", job.getId(), e.getMessage());
                handleFailure(job, e);
            }
        });
    }

    @RabbitListener(queues = "job.retry.queue")
    public void retryJob(JobMessage job) {
        log.info("Retrying job: {} type: {}", job.getId(), job.getType());
        job.setStatus(JobStatus.PROCESSING);
        job.setStartedAt(Instant.now());
        job.setAttempts(job.getAttempts() + 1);
        jobService.save(job);

        jobTaskExecutor.execute(() -> {
            try {
                process(job);
                job.setStatus(JobStatus.COMPLETED);
                job.setCompletedAt(Instant.now());
                jobService.save(job);
                log.info("Job retry completed: {}", job.getId());
            } catch (Exception e) {
                log.error("Job retry failed: {} error: {}", job.getId(), e.getMessage());
                handleFailure(job, e);
            }
        });
    }

    private void process(JobMessage job) throws InterruptedException {
        JobType type = job.getType();
        if (type == JobType.EXTRACT_TEXT) {
            log.info("Extracting text from document...");
            Thread.sleep(1000);
        } else if (type == JobType.GENERATE_THUMBNAIL) {
            log.info("Generating thumbnail...");
            Thread.sleep(1500);
        } else if (type == JobType.OCR) {
            log.info("Running OCR...");
            Thread.sleep(2000);
        } else if (type == JobType.CSV_VALIDATE) {
            log.info("Validating CSV...");
            Thread.sleep(800);
        } else if (type == JobType.CSV_IMPORT) {
            log.info("Importing CSV...");
            Thread.sleep(2000);
        } else {
            throw new IllegalArgumentException("Unsupported job type: " + type);
        }
    }

    private void handleFailure(JobMessage job, Exception e) {
        job.setStatus(JobStatus.FAILED);
        job.setErrorMessage(e.getMessage());
        job.setCompletedAt(Instant.now());
        jobService.save(job);

        if (job.getAttempts() < job.getMaxAttempts()) {
            job.setStatus(JobStatus.RETRYING);
            jobService.save(job);
            jobProducer.publishRetryJob(job);
        } else {
            job.setStatus(JobStatus.DEAD_LETTERED);
            jobService.save(job);
        }
    }
}
