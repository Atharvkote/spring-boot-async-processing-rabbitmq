package net.spring_boot.rabbitmq.queues.consumer;

import net.spring_boot.rabbitmq.queues.message.JobMessage;
import org.springframework.amqp.core.Message;

import java.io.IOException;

public interface JobConsumer {

    void processJob(JobMessage jobMessage, Message message, com.rabbitmq.client.Channel channel) throws IOException;
}
