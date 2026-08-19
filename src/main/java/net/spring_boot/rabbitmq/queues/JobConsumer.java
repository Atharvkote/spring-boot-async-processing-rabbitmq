package net.spring_boot.rabbitmq.queues;

import net.spring_boot.rabbitmq.model.JobMessage;

public interface JobConsumer {

    void processJob(JobMessage job);

    void retryJob(JobMessage job);
}
