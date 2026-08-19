package net.spring_boot.rabbitmq.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfiguration {

    @Bean
    public DirectExchange jobExchange() {
        return new DirectExchange("job.exchange", true, false);
    }

    @Bean
    public DirectExchange retryExchange() {
        return new DirectExchange("job.retry.exchange", true, false);
    }

    @Bean
    public Queue jobQueue() {
        return QueueBuilder.durable("job.queue")
                .withArgument("x-dead-letter-exchange", "job.exchange")
                .withArgument("x-dead-letter-routing-key", "job.dead")
                .build();
    }

    @Bean
    public Queue retry5sQueue() {
        return QueueBuilder.durable("job.retry.5s")
                .withArgument("x-dead-letter-exchange", "job.exchange")
                .withArgument("x-dead-letter-routing-key", "job.created")
                .withArgument("x-message-ttl", 5000)
                .build();
    }

    @Bean
    public Queue retry15sQueue() {
        return QueueBuilder.durable("job.retry.15s")
                .withArgument("x-dead-letter-exchange", "job.exchange")
                .withArgument("x-dead-letter-routing-key", "job.created")
                .withArgument("x-message-ttl", 15000)
                .build();
    }

    @Bean
    public Queue retry45sQueue() {
        return QueueBuilder.durable("job.retry.45s")
                .withArgument("x-dead-letter-exchange", "job.exchange")
                .withArgument("x-dead-letter-routing-key", "job.created")
                .withArgument("x-message-ttl", 45000)
                .build();
    }

    @Bean
    public Queue dlq() {
        return QueueBuilder.durable("job.dlq").build();
    }

    @Bean
    public Binding jobBinding(Queue jobQueue, DirectExchange jobExchange) {
        return BindingBuilder.bind(jobQueue).to(jobExchange).with("job.created");
    }

    @Bean
    public Binding retry5sBinding(Queue retry5sQueue, DirectExchange retryExchange) {
        return BindingBuilder.bind(retry5sQueue).to(retryExchange).with("job.retry.5s");
    }

    @Bean
    public Binding retry15sBinding(Queue retry15sQueue, DirectExchange retryExchange) {
        return BindingBuilder.bind(retry15sQueue).to(retryExchange).with("job.retry.15s");
    }

    @Bean
    public Binding retry45sBinding(Queue retry45sQueue, DirectExchange retryExchange) {
        return BindingBuilder.bind(retry45sQueue).to(retryExchange).with("job.retry.45s");
    }

    @Bean
    public Binding dlqBinding(Queue dlq, DirectExchange jobExchange) {
        return BindingBuilder.bind(dlq).to(jobExchange).with("job.dead");
    }

    @Bean
    public MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }
}
