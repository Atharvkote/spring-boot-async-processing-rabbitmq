package net.spring_boot.rabbitmq.repository;

import net.spring_boot.rabbitmq.models.StoredFile;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface StoredFileRepository extends JpaRepository<StoredFile, UUID> {
}
