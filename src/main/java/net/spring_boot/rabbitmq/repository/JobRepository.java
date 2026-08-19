package net.spring_boot.rabbitmq.repository;

import net.spring_boot.rabbitmq.models.Job;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {
}
