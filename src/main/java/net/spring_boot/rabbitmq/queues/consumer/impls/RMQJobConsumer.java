package net.spring_boot.rabbitmq.queues.consumer.impls;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import net.spring_boot.rabbitmq.models.Job;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.queues.consumer.JobConsumer;
import net.spring_boot.rabbitmq.queues.message.JobMessage;
import net.spring_boot.rabbitmq.queues.producer.JobProducer;
import net.spring_boot.rabbitmq.service.helpers.JobProcessor;
import net.spring_boot.rabbitmq.service.JobService;
import net.spring_boot.rabbitmq.service.helpers.JobStateManger;
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
    private final JobStateManger jobStateManger;
    private final JobProducer jobProducer;
    private final JobProcessor jobProcessor;
    private final ObjectMapper objectMapper;

    public RMQJobConsumer(JobService jobService, JobStateManger jobStateManger,
                          JobProducer jobProducer, JobProcessor jobProcessor,
                          ObjectMapper objectMapper) {
        this.jobService = jobService;
        this.jobStateManger = jobStateManger;
        this.jobProducer = jobProducer;
        this.jobProcessor = jobProcessor;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "job.queue", ackMode = "MANUAL")
    public void processJob(JobMessage jobMessage, Message message, Channel channel) throws IOException {
        UUID jobId = jobMessage.id();
        log.info("Received job id={} attempt={}", jobId, jobMessage.attempt());

        Job job = jobService.findById(jobId);

        if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.DEAD_LETTERED) {
            log.info("Job id={} already in terminal state {}, acknowledging", jobId, job.getStatus());
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            return;
        }

        try {
            jobStateManger.validateTransition(job.getStatus(), JobStatus.PROCESSING);
            job.setStatus(JobStatus.PROCESSING);
            job.setAttempts(job.getAttempts() + 1);
            job.setStartedAt(Instant.now());
            jobService.save(job);
            jobService.recordHistory(job, null, JobStatus.PROCESSING, job.getAttempts(), "Processing started");

            jobProcessor.process(job);

            jobStateManger.validateTransition(job.getStatus(), JobStatus.COMPLETED);
            job.setStatus(JobStatus.COMPLETED);
            job.setCompletedAt(Instant.now());
            jobService.save(job);
            jobService.recordHistory(job, JobStatus.PROCESSING, JobStatus.COMPLETED, job.getAttempts(), "Processing completed successfully");

            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            log.info("Job id={} completed successfully", jobId);
        } catch (Exception e) {
            log.error("Job id={} failed: {}", jobId, e.getMessage());
            job.setErrorMessage(e.getMessage());
            jobService.save(job);
            jobService.recordHistory(job, JobStatus.PROCESSING, JobStatus.FAILED, job.getAttempts(),
                    "Processing failed: " + e.getMessage());

            if (job.getAttempts() < job.getMaxAttempts()) {
                jobStateManger.validateTransition(JobStatus.FAILED, JobStatus.RETRYING);
                job.setStatus(JobStatus.RETRYING);
                jobService.save(job);
                jobService.recordHistory(job, JobStatus.FAILED, JobStatus.RETRYING, job.getAttempts(),
                        "Scheduling retry attempt " + (job.getAttempts() + 1));

                JobMessage retryMessage = new JobMessage(
                        job.getId(), job.getType(), job.getAttempts() + 1, job.getMaxAttempts(), Instant.now());
                jobProducer.publishRetryJob(retryMessage, job.getAttempts() + 1);

                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                log.info("Job id={} failed. Scheduling retry attempt={}", jobId, job.getAttempts() + 1);
            } else {
                jobStateManger.validateTransition(JobStatus.FAILED, JobStatus.DEAD_LETTERED);
                job.setStatus(JobStatus.DEAD_LETTERED);
                job.setCompletedAt(Instant.now());
                jobService.save(job);
                jobService.recordHistory(job, JobStatus.FAILED, JobStatus.DEAD_LETTERED, job.getAttempts(),
                        "Max attempts exceeded, sending to DLQ");

                JobMessage dlqMessage = new JobMessage(
                        job.getId(), job.getType(), job.getAttempts(), job.getMaxAttempts(), Instant.now());
                jobProducer.publishToDlq(dlqMessage);

                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                log.info("Job id={} exceeded max attempts. Sending to DLQ", jobId);
            }
        }
    }
}
