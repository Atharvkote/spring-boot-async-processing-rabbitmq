package net.spring_boot.rabbitmq.service.impls;

import net.spring_boot.rabbitmq.config.properties.FileStorageProperties;
import net.spring_boot.rabbitmq.service.FileStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@Service
public class LocalFileStorageService implements FileStorageService {

    private static final Logger log = LoggerFactory.getLogger(LocalFileStorageService.class);

    private final Path root;

    public LocalFileStorageService(FileStorageProperties properties) {
        this.root = Paths.get(properties.getDirectory()).toAbsolutePath().normalize();
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new RuntimeException("Could not create storage directory", e);
        }
    }

    @Override
    public String save(MultipartFile file) throws IOException {
        String originalFilename = file.getOriginalFilename();
        if (originalFilename == null || originalFilename.isBlank()) {
            throw new IOException("Invalid file name");
        }

        String extension = "";
        int dotIndex = originalFilename.lastIndexOf('.');
        if (dotIndex > 0) {
            extension = originalFilename.substring(dotIndex);
        }

        String storageFilename = UUID.randomUUID() + extension;
        Path destination = root.resolve(storageFilename).normalize();

        if (!destination.startsWith(root)) {
            throw new IOException("Path traversal detected: " + storageFilename);
        }

        Files.copy(file.getInputStream(), destination);
        log.info("Saved file {} to {}", originalFilename, destination);
        return storageFilename;
    }

    @Override
    public byte[] read(String fileName) throws IOException {
        Path file = root.resolve(fileName).normalize();
        if (!file.startsWith(root)) {
            throw new IOException("Path traversal detected: " + fileName);
        }
        return Files.readAllBytes(file);
    }

    @Override
    public void delete(String fileName) throws IOException {
        Path file = root.resolve(fileName).normalize();
        if (!file.startsWith(root)) {
            throw new IOException("Path traversal detected: " + fileName);
        }
        Files.deleteIfExists(file);
    }

    @Override
    public boolean exists(String fileName) {
        try {
            Path file = root.resolve(fileName).normalize();
            return Files.exists(file) && file.startsWith(root);
        } catch (Exception e) {
            return false;
        }
    }
}
