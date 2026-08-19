package net.spring_boot.rabbitmq.queues.producer.impls;

import net.spring_boot.rabbitmq.config.properties.JobProperties;
import net.spring_boot.rabbitmq.queues.message.JobMessage;
import net.spring_boot.rabbitmq.queues.producer.JobProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RMQJobProducer implements JobProducer {

    private static final Logger log = LoggerFactory.getLogger(RMQJobProducer.class);

    private final RabbitTemplate rabbitTemplate;
    private final JobProperties jobProperties;

    public RMQJobProducer(RabbitTemplate rabbitTemplate, JobProperties jobProperties) {
        this.rabbitTemplate = rabbitTemplate;
        this.jobProperties = jobProperties;
    }

    @Override
    public void publishJob(JobMessage jobMessage) {
        rabbitTemplate.convertAndSend("job.exchange", "job.created", jobMessage, message -> {
            message.getMessageProperties().setMessageId(jobMessage.id().toString());
            message.getMessageProperties().setContentType("application/json");
            return message;
        });
        log.info("Publishing job id={} routingKey=job.created", jobMessage.id());
    }

    @Override
    public void publishRetryJob(JobMessage jobMessage, int attempt) {
        List<Long> delays = jobProperties.getRetry().getDelays();
        long delay = delays.get(Math.min(attempt - 1, delays.size() - 1));
        String routingKey = "job.retry." + delay + "ms";

        rabbitTemplate.convertAndSend("job.retry.exchange", routingKey, jobMessage, message -> {
            message.getMessageProperties().setMessageId(jobMessage.id().toString());
            message.getMessageProperties().setContentType("application/json");
            return message;
        });
        log.info("Publishing retry job id={} attempt={} routingKey={} delay={}ms",
                jobMessage.id(), attempt, routingKey, delay);
    }

    @Override
    public void publishToDlq(JobMessage jobMessage) {
        rabbitTemplate.convertAndSend("job.exchange", "job.dead", jobMessage, message -> {
            message.getMessageProperties().setMessageId(jobMessage.id().toString());
            message.getMessageProperties().setContentType("application/json");
            return message;
        });
        log.info("Publishing job id={} to DLQ", jobMessage.id());
    }
}
