package net.spring_boot.rabbitmq.queues;

import net.spring_boot.rabbitmq.model.JobMessage;

public interface JobProducer {

    void publishJob(JobMessage job);

    void publishRetryJob(JobMessage job);
}
