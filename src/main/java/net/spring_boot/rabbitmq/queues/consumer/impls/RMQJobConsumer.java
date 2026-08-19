package net.spring_boot.rabbitmq.queues.consumer.impls;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.models.Job;
import net.spring_boot.rabbitmq.models.StoredFile;
import net.spring_boot.rabbitmq.queues.consumer.JobConsumer;
import net.spring_boot.rabbitmq.queues.message.JobMessage;
import net.spring_boot.rabbitmq.queues.producer.JobProducer;
import net.spring_boot.rabbitmq.repository.StoredFileRepository;
import net.spring_boot.rabbitmq.service.FileStorageService;
import net.spring_boot.rabbitmq.service.JobService;
import net.spring_boot.rabbitmq.service.helpers.JobProcessor;
import net.spring_boot.rabbitmq.service.helpers.JobStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

@Component
public class RMQJobConsumer implements JobConsumer {

    private static final Logger log = LoggerFactory.getLogger(RMQJobConsumer.class);

    private final JobService jobService;
    private final JobStateManager jobStateManager;
    private final JobProducer jobProducer;
    private final JobProcessor jobProcessor;
    private final StoredFileRepository storedFileRepository;
    private final FileStorageService fileStorageService;
    private final ObjectMapper objectMapper;

    public RMQJobConsumer(JobService jobService, JobStateManager jobStateManager,
                          JobProducer jobProducer, JobProcessor jobProcessor,
                          StoredFileRepository storedFileRepository,
                          FileStorageService fileStorageService,
                          ObjectMapper objectMapper) {
        this.jobService = jobService;
        this.jobStateManager = jobStateManager;
        this.jobProducer = jobProducer;
        this.jobProcessor = jobProcessor;
        this.storedFileRepository = storedFileRepository;
        this.fileStorageService = fileStorageService;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "job.queue", ackMode = "MANUAL")
    public void processJob(JobMessage jobMessage, Message message, Channel channel) throws IOException {
        UUID jobId = jobMessage.id();
        UUID fileId = jobMessage.fileId();
        log.info("Received job jobId={} fileId={} attempt={}", jobId, fileId, jobMessage.attempt());

        Job job = jobService.findById(jobId);
        StoredFile storedFile = storedFileRepository.findById(fileId)
                .orElseThrow(() -> new IllegalArgumentException("File not found: " + fileId));

        if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.DEAD_LETTERED) {
            log.info("Job jobId={} already in terminal state {}, acknowledging", jobId, job.getStatus());
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            return;
        }

        try {
            jobStateManager.validateTransition(job.getStatus(), JobStatus.PROCESSING);
            job.setStatus(JobStatus.PROCESSING);
            job.setAttempts(job.getAttempts() + 1);
            job.setStartedAt(Instant.now());
            jobService.save(job);
            jobService.recordHistory(job, null, JobStatus.PROCESSING, job.getAttempts(), "Processing started");

            byte[] fileBytes = fileStorageService.read(storedFile.getStoragePath());
            jobProcessor.process(job, storedFile, fileBytes);

            jobStateManager.validateTransition(job.getStatus(), JobStatus.COMPLETED);
            job.setStatus(JobStatus.COMPLETED);
            job.setCompletedAt(Instant.now());
            jobService.save(job);
            jobService.recordHistory(job, JobStatus.PROCESSING, JobStatus.COMPLETED, job.getAttempts(), "Processing completed successfully");

            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            log.info("File processed successfully fileId={} jobId={}", fileId, jobId);
        } catch (Exception e) {
            log.error("File processing failed fileId={} jobId={} error={}", fileId, jobId, e.getMessage());
            job.setErrorMessage(e.getMessage());
            jobService.save(job);
            jobService.recordHistory(job, JobStatus.PROCESSING, JobStatus.FAILED, job.getAttempts(),
                    "Processing failed: " + e.getMessage());

            if (job.getAttempts() < job.getMaxAttempts()) {
                jobStateManager.validateTransition(JobStatus.FAILED, JobStatus.RETRYING);
                job.setStatus(JobStatus.RETRYING);
                jobService.save(job);
                jobService.recordHistory(job, JobStatus.FAILED, JobStatus.RETRYING, job.getAttempts(),
                        "Scheduling retry attempt " + (job.getAttempts() + 1));

                JobMessage retryMessage = new JobMessage(
                        job.getId(), fileId, job.getType(), job.getAttempts() + 1, job.getMaxAttempts(), Instant.now());
                jobProducer.publishRetryJob(retryMessage, job.getAttempts() + 1);

                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                log.info("File processing failed fileId={} jobId={} retryAttempt={}", fileId, jobId, job.getAttempts() + 1);
            } else {
                jobStateManager.validateTransition(JobStatus.FAILED, JobStatus.DEAD_LETTERED);
                job.setStatus(JobStatus.DEAD_LETTERED);
                job.setCompletedAt(Instant.now());
                jobService.save(job);
                jobService.recordHistory(job, JobStatus.FAILED, JobStatus.DEAD_LETTERED, job.getAttempts(),
                        "Max attempts exceeded, sending to DLQ");

                JobMessage dlqMessage = new JobMessage(
                        job.getId(), fileId, job.getType(), job.getAttempts(), job.getMaxAttempts(), Instant.now());
                jobProducer.publishToDlq(dlqMessage);

                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                log.info("File processing exhausted retries fileId={} jobId={}", fileId, jobId);
            }
        }
    }
}
