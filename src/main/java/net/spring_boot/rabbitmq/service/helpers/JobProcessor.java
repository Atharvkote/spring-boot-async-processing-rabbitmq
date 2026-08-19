package net.spring_boot.rabbitmq.service.helpers;

import net.spring_boot.rabbitmq.config.properties.JobProperties;
import net.spring_boot.rabbitmq.models.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class JobProcessor {

    private static final Logger log = LoggerFactory.getLogger(JobProcessor.class);

    private final JobProperties jobProperties;
    private final Random random = new Random();

    public JobProcessor(JobProperties jobProperties) {
        this.jobProperties = jobProperties;
    }

    public void process(Job job) throws InterruptedException {
        long delayMs = jobProperties.getProcessing().getDelayMs();
        double failureRate = jobProperties.getProcessing().getFailureRate();

        log.info("Processing job id={} delayMs={} failureRate={}", job.getId(), delayMs, failureRate);
        Thread.sleep(delayMs);

        if (random.nextDouble() < failureRate) {
            throw new RuntimeException("Simulated processing failure for job " + job.getId());
        }

        log.info("Processing completed for job id={}", job.getId());
    }
}
