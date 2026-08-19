package net.spring_boot.rabbitmq.enums;

public enum JobStatus {
    QUEUED,
    PROCESSING,
    COMPLETED,
    FAILED,
    RETRYING,
    DEAD_LETTERED
}
