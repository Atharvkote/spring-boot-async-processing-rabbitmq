package net.spring_boot.rabbitmq.config;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class RabbitMQConfig {

    @Value("${rabbitmq.queue.job}")
    private String jobQueue;

    @Value("${rabbitmq.queue.retry}")
    private String retryQueue;

    @Value("${rabbitmq.queue.dlq}")
    private String dlq;

    @Value("${rabbitmq.exchange.job}")
    private String jobExchange;

    @Value("${rabbitmq.routing.key.job}")
    private String jobRoutingKey;

    @Value("${rabbitmq.routing.key.retry}")
    private String retryRoutingKey;

    @Value("${rabbitmq.routing.key.dlq}")
    private String dlqRoutingKey;

    @Bean
    public DirectExchange jobExchange() {
        return new DirectExchange(jobExchange);
    }

    @Bean
    public Queue jobQueue() {
        return QueueBuilder.durable(jobQueue)
                .withArgument("x-dead-letter-exchange", jobExchange)
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey)
                .build();
    }

    @Bean
    public Queue retryQueue() {
        return QueueBuilder.durable(retryQueue)
                .withArgument("x-dead-letter-exchange", jobExchange)
                .withArgument("x-dead-letter-routing-key", jobRoutingKey)
                .build();
    }

    @Bean
    public Queue dlq() {
        return QueueBuilder.durable(dlq).build();
    }

    @Bean
    public Binding jobBinding(Queue jobQueue, DirectExchange jobExchange) {
        return BindingBuilder.bind(jobQueue).to(jobExchange).with(jobRoutingKey);
    }

    @Bean
    public Binding retryBinding(Queue retryQueue, DirectExchange jobExchange) {
        return BindingBuilder.bind(retryQueue).to(jobExchange).with(retryRoutingKey);
    }

    @Bean
    public Binding dlqBinding(Queue dlq, DirectExchange jobExchange) {
        return BindingBuilder.bind(dlq).to(jobExchange).with(dlqRoutingKey);
    }

    @Bean("jobTaskExecutor")
    public ThreadPoolTaskExecutor jobTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("job-worker-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }
}
