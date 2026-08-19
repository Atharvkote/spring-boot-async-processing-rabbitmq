package net.spring_boot.rabbitmq.service;

import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

public interface FileStorageService {

    String save(MultipartFile file) throws IOException;

    byte[] read(String fileName) throws IOException;

    void delete(String fileName) throws IOException;

    boolean exists(String fileName);
}
