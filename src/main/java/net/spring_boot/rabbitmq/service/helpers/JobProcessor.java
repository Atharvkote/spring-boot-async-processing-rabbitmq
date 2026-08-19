package net.spring_boot.rabbitmq.service.helpers;

import net.spring_boot.rabbitmq.config.properties.JobProperties;
import net.spring_boot.rabbitmq.models.Job;
import net.spring_boot.rabbitmq.models.StoredFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Random;

@Component
public class JobProcessor {

    private static final Logger log = LoggerFactory.getLogger(JobProcessor.class);

    private final JobProperties jobProperties;
    private final Random random = new Random();

    public JobProcessor(JobProperties jobProperties) {
        this.jobProperties = jobProperties;
    }

    public void process(Job job, StoredFile storedFile, byte[] fileBytes) throws Exception {
        long delayMs = jobProperties.getProcessing().getDelayMs();
        double failureRate = jobProperties.getProcessing().getFailureRate();

        log.info("Processing file fileId={} jobId={} size={} delayMs={}", storedFile.getId(), job.getId(), fileBytes.length, delayMs);
        Thread.sleep(delayMs);

        String checksum = calculateChecksum(fileBytes);
        log.info("File checksum calculated fileId={} checksum={}", storedFile.getId(), checksum);

        if (random.nextDouble() < failureRate) {
            throw new RuntimeException("Simulated processing failure for job " + job.getId());
        }

        log.info("File processed successfully fileId={} jobId={}", storedFile.getId(), job.getId());
    }

    private String calculateChecksum(byte[] data) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(data);
        return HexFormat.of().formatHex(hash);
    }
}
