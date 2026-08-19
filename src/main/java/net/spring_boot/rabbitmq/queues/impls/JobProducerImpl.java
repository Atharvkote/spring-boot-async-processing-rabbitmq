package net.spring_boot.rabbitmq.queues.impls;

import net.spring_boot.rabbitmq.model.JobMessage;
import net.spring_boot.rabbitmq.queues.JobProducer;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class JobProducerImpl implements JobProducer {

    private final RabbitTemplate rabbitTemplate;
    private final String jobExchange;
    private final String jobRoutingKey;
    private final String retryRoutingKey;

    public JobProducerImpl(RabbitTemplate rabbitTemplate,
                           @Value("${rabbitmq.exchange.job}") String jobExchange,
                           @Value("${rabbitmq.routing.key.job}") String jobRoutingKey,
                           @Value("${rabbitmq.routing.key.retry}") String retryRoutingKey) {
        this.rabbitTemplate = rabbitTemplate;
        this.jobExchange = jobExchange;
        this.jobRoutingKey = jobRoutingKey;
        this.retryRoutingKey = retryRoutingKey;
    }

    @Override
    public void publishJob(JobMessage job) {
        rabbitTemplate.convertAndSend(
                jobExchange,
                jobRoutingKey,
                job,
                message -> {
                    message.getMessageProperties().setMessageId(job.getId().toString());
                    message.getMessageProperties().setContentType("application/json");
                    return message;
                }
        );
    }

    @Override
    public void publishRetryJob(JobMessage job) {
        rabbitTemplate.convertAndSend(
                jobExchange,
                retryRoutingKey,
                job,
                message -> {
                    message.getMessageProperties().setMessageId(job.getId().toString());
                    message.getMessageProperties().setContentType("application/json");
                    return message;
                }
        );
    }
}
