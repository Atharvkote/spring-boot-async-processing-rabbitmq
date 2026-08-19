package net.spring_boot.rabbitmq.repository;

import net.spring_boot.rabbitmq.models.JobHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface JobHistoryRepository extends JpaRepository<JobHistory, UUID> {
}
