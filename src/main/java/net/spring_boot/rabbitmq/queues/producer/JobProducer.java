package net.spring_boot.rabbitmq.queues.producer;

import net.spring_boot.rabbitmq.queues.message.JobMessage;

public interface JobProducer {

    void publishJob(JobMessage jobMessage);

    void publishRetryJob(JobMessage jobMessage, int attempt);

    void publishToDlq(JobMessage jobMessage);
}
