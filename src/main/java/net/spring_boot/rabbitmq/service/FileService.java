package net.spring_boot.rabbitmq.service;

import net.spring_boot.rabbitmq.models.Job;
import net.spring_boot.rabbitmq.models.StoredFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

public interface FileService {

    StoredFile upload(MultipartFile file) throws IOException;

    StoredFile findById(UUID id);

    Collection<StoredFile> findAll();

    byte[] readFile(String storagePath) throws IOException;

    void deleteFile(UUID id) throws IOException;
}
