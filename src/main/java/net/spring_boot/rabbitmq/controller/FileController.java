package net.spring_boot.rabbitmq.controller;

import jakarta.validation.constraints.NotNull;
import net.spring_boot.rabbitmq.dto.FileResponse;
import net.spring_boot.rabbitmq.exception.BadRequestException;
import net.spring_boot.rabbitmq.models.StoredFile;
import net.spring_boot.rabbitmq.service.FileService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/files")
public class FileController {

    private final FileService fileService;

    public FileController(FileService fileService) {
        this.fileService = fileService;
    }

    @PostMapping
    public ResponseEntity<FileResponse> uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            StoredFile storedFile = fileService.upload(file);
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(new FileResponse(
                            storedFile.getId(),
                            storedFile.getOriginalName(),
                            storedFile.getContentType(),
                            storedFile.getFileSize(),
                            storedFile.getCreatedAt()
                    ));
        } catch (IOException e) {
            throw new BadRequestException("Failed to upload file: " + e.getMessage());
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<FileResponse> getFile(@PathVariable UUID id) {
        StoredFile storedFile = fileService.findById(id);
        return ResponseEntity.ok(new FileResponse(
                storedFile.getId(),
                storedFile.getOriginalName(),
                storedFile.getContentType(),
                storedFile.getFileSize(),
                storedFile.getCreatedAt()
        ));
    }

    @GetMapping
    public ResponseEntity<Collection<FileResponse>> getAllFiles() {
        Collection<StoredFile> files = fileService.findAll();
        Collection<FileResponse> responses = files.stream()
                .map(f -> new FileResponse(
                        f.getId(),
                        f.getOriginalName(),
                        f.getContentType(),
                        f.getFileSize(),
                        f.getCreatedAt()
                ))
                .collect(Collectors.toList());
        return ResponseEntity.ok(responses);
    }

    @GetMapping("/{id}/download")
    public ResponseEntity<byte[]> downloadFile(@PathVariable UUID id) throws IOException {
        StoredFile storedFile = fileService.findById(id);
        byte[] data = fileService.readFile(storedFile.getStoragePath());

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + storedFile.getOriginalName() + "\"")
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(data);
    }
}
