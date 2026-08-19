package net.spring_boot.rabbitmq.service.impls;

import net.spring_boot.rabbitmq.config.properties.JobProperties;
import net.spring_boot.rabbitmq.enums.JobStatus;
import net.spring_boot.rabbitmq.enums.JobType;
import net.spring_boot.rabbitmq.exception.ResourceNotFoundException;
import net.spring_boot.rabbitmq.models.Job;
import net.spring_boot.rabbitmq.models.JobHistory;
import net.spring_boot.rabbitmq.models.StoredFile;
import net.spring_boot.rabbitmq.queues.message.JobMessage;
import net.spring_boot.rabbitmq.queues.producer.JobProducer;
import net.spring_boot.rabbitmq.repository.JobHistoryRepository;
import net.spring_boot.rabbitmq.repository.JobRepository;
import net.spring_boot.rabbitmq.repository.StoredFileRepository;
import net.spring_boot.rabbitmq.service.FileService;
import net.spring_boot.rabbitmq.service.FileStorageService;
import net.spring_boot.rabbitmq.service.helpers.JobStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;

@Service
public class FileServiceImpl implements FileService {

    private static final Logger log = LoggerFactory.getLogger(FileServiceImpl.class);

    private final StoredFileRepository storedFileRepository;
    private final JobRepository jobRepository;
    private final JobHistoryRepository jobHistoryRepository;
    private final FileStorageService fileStorageService;
    private final JobProperties jobProperties;
    private final JobStateManager jobStateManager;
    private final JobProducer jobProducer;

    public FileServiceImpl(StoredFileRepository storedFileRepository,
                           JobRepository jobRepository,
                           JobHistoryRepository jobHistoryRepository,
                           FileStorageService fileStorageService,
                           JobProperties jobProperties,
                           JobStateManager jobStateManager,
                           JobProducer jobProducer) {
        this.storedFileRepository = storedFileRepository;
        this.jobRepository = jobRepository;
        this.jobHistoryRepository = jobHistoryRepository;
        this.fileStorageService = fileStorageService;
        this.jobProperties = jobProperties;
        this.jobStateManager = jobStateManager;
        this.jobProducer = jobProducer;
    }

    @Override
    public StoredFile upload(MultipartFile file) throws IOException {
        String storagePath = fileStorageService.save(file);

        StoredFile storedFile = new StoredFile();
        storedFile.setOriginalName(file.getOriginalFilename());
        storedFile.setStoredName(storagePath);
        storedFile.setStoragePath(storagePath);
        storedFile.setContentType(file.getContentType());
        storedFile.setFileSize(file.getSize());
        StoredFile savedFile = storedFileRepository.save(storedFile);

        log.info("File uploaded fileId={} originalName={} storagePath={}", savedFile.getId(), savedFile.getOriginalName(), savedFile.getStoragePath());

        Job job = new Job();
        job.setFileId(savedFile.getId());
        job.setType(JobType.FILE_ANALYSIS);
        job.setMaxAttempts(3);
        job.setStatus(net.spring_boot.rabbitmq.enums.JobStatus.QUEUED);
        Job savedJob = jobRepository.save(job);
        jobStateManager.validateTransition(null, net.spring_boot.rabbitmq.enums.JobStatus.QUEUED);
        recordHistory(savedJob, null, net.spring_boot.rabbitmq.enums.JobStatus.QUEUED, 0, "Processing job created");

        JobMessage jobMessage = new JobMessage(
                savedJob.getId(),
                savedFile.getId(),
                savedJob.getType(),
                1,
                savedJob.getMaxAttempts(),
                Instant.now()
        );
        jobProducer.publishJob(jobMessage);

        log.info("Created processing job jobId={} fileId={}", savedJob.getId(), savedFile.getId());

        return savedFile;
    }

    @Override
    public StoredFile findById(UUID id) {
        return storedFileRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("File not found: " + id));
    }

    @Override
    public Collection<StoredFile> findAll() {
        return storedFileRepository.findAll();
    }

    @Override
    public byte[] readFile(String storagePath) throws IOException {
        return fileStorageService.read(storagePath);
    }

    @Override
    public void deleteFile(UUID id) throws IOException {
        StoredFile storedFile = findById(id);
        fileStorageService.delete(storedFile.getStoragePath());
        storedFileRepository.delete(storedFile);
    }

    private void recordHistory(Job job, JobStatus previousStatus, JobStatus newStatus, int attempt, String message) {
        JobHistory history = new JobHistory();
        history.setJob(job);
        history.setPreviousStatus(previousStatus);
        history.setNewStatus(newStatus);
        history.setAttempt(attempt);
        history.setMessage(message);
        jobHistoryRepository.save(history);
    }
}
