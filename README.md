# Spring Boot RabbitMQ Async File Processing Demo

A backend-only Spring Boot 3.x + RabbitMQ asynchronous file processing learning demo. It demonstrates core RabbitMQ concepts using a file upload REST API, PostgreSQL persistence, local file storage, and Flyway migrations.

## Learning Objectives

- Multipart file upload
- File storage abstraction
- PostgreSQL file metadata
- Producer / Consumer pattern
- Exchange, routing keys, durable queues
- Manual acknowledgement
- Consumer concurrency and prefetch
- Dead-letter exchange (DLX) and dead-letter queue (DLQ)
- Delayed retry with TTL
- Idempotent processing
- Job lifecycle and state transitions
- Separation between file storage and processing

## Architecture

```text
              Upload File
                   │
                   ▼
            FileController
                   │
                   ▼
          FileStorageService
                   │
                   ▼
             ./storage
                   │
                   ▼
            PostgreSQL
                   │
                   ▼
            Create Job
                   │
                   ▼
         RabbitMQ Producer
                   │
                   ▼
           job.exchange
                   │
                   ▼
             job.queue
                   │
                   ▼
            Job Consumer
                   │
                   ▼
           Load File
                   │
                   ▼
      FileStorageService.read()
                   │
                   ▼
          Process File
              │       │
           success   failure
              │       │
              ▼       ▼
          COMPLETED  RETRY
                       │
                       ▼
                    TTL
                       │
                       ▼
                  job.queue
                       │
                       ▼
                     DLQ
```

## RabbitMQ Topology

| Name | Type | Purpose |
|------|------|---------|
| `job.exchange` | Direct | Main exchange for jobs |
| `job.retry.exchange` | Direct | Retry exchange |
| `job.queue` | Durable | Main processing queue |
| `job.retry.5s` | Durable | 5-second delayed retry |
| `job.retry.15s` | Durable | 15-second delayed retry |
| `job.retry.45s` | Durable | 45-second delayed retry |
| `job.dlq` | Durable | Dead-letter queue |

### Routing Keys

- `job.created` → `job.queue`
- `job.retry.5s` → `job.retry.5s`
- `job.retry.15s` → `job.retry.15s`
- `job.retry.45s` → `job.retry.45s`
- `job.dead` → `job.dlq`

## Message Lifecycle

### Successful Processing

```text
POST /api/files
    ↓
File stored in ./storage
    ↓
File metadata saved in PostgreSQL
    ↓
Processing job created (QUEUED)
    ↓
JobMessage published to job.exchange (routing key: job.created)
    ↓
Delivered to job.queue
    ↓
Consumer receives message
    ↓
Load job from PostgreSQL
    ↓
Load file from FileStorageService
    ↓
Read file bytes
    ↓
JobProcessor simulates work
    ↓
Status → COMPLETED
    ↓
basicAck
```

### Failure and Retry

```text
Consumer receives message
    ↓
Load job
    ↓
Load file
    ↓
Read file bytes
    ↓
JobProcessor throws exception
    ↓
Status → FAILED
    ↓
attempt < maxAttempts?
    ↓
YES
    ↓
Status → RETRYING
    ↓
Message published to job.retry.exchange
    ↓
Routed to job.retry.5s (or 15s / 45s)
    ↓
TTL expires
    ↓
Dead-lettered back to job.exchange
    ↓
Routed to job.queue (routing key: job.created)
    ↓
Consumer receives message again
    ↓
attempt++
    ↓
...
    ↓
NO (max attempts reached)
    ↓
Status → DEAD_LETTERED
    ↓
Message published to job.dlq
    ↓
basicAck
```

## Manual ACK

The consumer uses `ackMode = "MANUAL"`. The message is acknowledged only after the processing decision is complete:

- Success → `basicAck`
- Retry scheduled → `basicAck` original message
- DLQ → `basicAck` original message

This prevents uncontrolled `requeue=true` loops.

## Prefetch

Configured via `spring.rabbitmq.listener.simple.prefetch: 10`.

Prefetch controls how many unacknowledged messages can be delivered to a single consumer at once. With prefetch=10, a consumer can hold up to 10 messages before the broker sends more.

## Consumer Concurrency

Configured via:
- `concurrency: 2` (minimum consumers)
- `max-concurrency: 5` (maximum consumers)

Multiple consumers process jobs in parallel. RabbitMQ distributes messages across available consumers.

## Idempotency

When a message arrives, the consumer loads the job from PostgreSQL by ID. If the job is already `COMPLETED` or `DEAD_LETTERED`, the message is acknowledged immediately without reprocessing. This handles duplicate message delivery.

## Job State Machine

Valid transitions:

| From | To |
|------|----|
| QUEUED | PROCESSING |
| PROCESSING | COMPLETED |
| PROCESSING | FAILED |
| FAILED | RETRYING |
| FAILED | DEAD_LETTERED |
| RETRYING | PROCESSING |
| DEAD_LETTERED | QUEUED |

Invalid transitions throw `InvalidStateTransitionException`.

## Database

### stored_files

| Column | Type |
|--------|------|
| id | UUID |
| original_name | VARCHAR(255) |
| stored_name | VARCHAR(255) |
| storage_path | VARCHAR(500) |
| content_type | VARCHAR(100) |
| file_size | BIGINT |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |

### processing_jobs

| Column | Type |
|--------|------|
| id | UUID |
| file_id | UUID |
| type | VARCHAR(50) |
| status | VARCHAR(50) |
| attempts | INTEGER |
| max_attempts | INTEGER |
| error_message | TEXT |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| started_at | TIMESTAMP |
| completed_at | TIMESTAMP |

### job_history

| Column | Type |
|--------|------|
| id | UUID |
| job_id | UUID (FK) |
| previous_status | VARCHAR(50) |
| new_status | VARCHAR(50) |
| attempt | INTEGER |
| message | TEXT |
| created_at | TIMESTAMP |

Flyway owns the schema. JPA `ddl-auto` is set to `validate`.

## File Storage

```text
Application
     ↓
FileStorageService (interface)
     ↓
LocalFileStorageService (implementation)
     ↓
./storage/
```

Files are stored with UUID-based filenames. Path traversal is prevented. The abstraction allows future replacement with S3 without changing application code.

## API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/files` | Upload a file |
| GET | `/api/files` | List all files |
| GET | `/api/files/{id}` | Get file metadata |
| GET | `/api/files/{id}/download` | Download file |
| GET | `/api/jobs/{id}` | Get job by ID |
| GET | `/api/jobs/{id}/history` | Get job history |
| GET | `/api/jobs` | List all jobs |
| POST | `/api/jobs/{id}/retry` | Manually retry a failed job |

### Upload File

```bash
curl -X POST http://localhost:8080/api/files \
  -F "file=@/path/to/report.pdf"
```

### Manual Retry

```text
POST /api/jobs/{id}/retry
```

Resets a `FAILED` or `DEAD_LETTERED` job and re-queues it.

## Local Setup

```bash
# Start infrastructure
docker compose up -d

# Run the application
./mvnw spring-boot:run
```

Or with your local PostgreSQL and RabbitMQ:

```bash
./mvnw clean package
java -jar target/*.jar
```

## Manual Verification

1. **Upload a file**
   ```bash
   curl -X POST http://localhost:8080/api/files \
     -F "file=@/path/to/report.pdf"
   ```

2. **Observe RabbitMQ Management UI** (http://localhost:15672)
   - Check `job.queue` for the new message
   - Check `job.exchange` bindings

3. **Observe consumer logs**
   - Consumer picks up the job
   - Status changes to `PROCESSING`
   - File is read from storage

4. **Check file metadata**
   ```bash
   curl http://localhost:8080/api/files/{id}
   ```

5. **Check job status**
   ```bash
   curl http://localhost:8080/api/jobs/{id}
   ```

6. **Verify job history**
   ```bash
   curl http://localhost:8080/api/jobs/{id}/history
   ```

7. **Trigger failure**
   - With `failure-rate: 0.3`, roughly 30% of jobs fail
   - Failed jobs go to `RETRYING` state
   - Message is published to `job.retry.5s`

8. **Observe retry**
   - After 5 seconds, TTL expires
   - Message returns to `job.queue`
   - Consumer reprocesses with `attempt=2`

9. **Verify DLQ**
   - After 3 failed attempts, job goes to `DEAD_LETTERED`
   - Message appears in `job.dlq`

10. **Verify idempotency**
    - If a duplicate message arrives for a completed job, it is acknowledged without reprocessing

11. **Download file**
    ```bash
    curl http://localhost:8080/api/files/{id}/download -o downloaded.pdf
    ```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `spring.rabbitmq.host` | localhost | RabbitMQ host |
| `spring.rabbitmq.port` | 5672 | RabbitMQ port |
| `SPRING_DATASOURCE_URL` | jdbc:postgresql://localhost:5432/rabbitmq_demo | PostgreSQL JDBC URL |
| `SPRING_DATASOURCE_USERNAME` | postgres | PostgreSQL username |
| `SPRING_DATASOURCE_PASSWORD` | postgres | PostgreSQL password |
| `job.processing.delay-ms` | 3000 | Simulated processing delay |
| `job.processing.failure-rate` | 0.3 | Simulated failure rate |
| `job.retry.delays` | [5000, 15000, 45000] | Retry delays in ms |
| `file-storage.directory` | ./storage | Local file storage path |
| `spring.servlet.multipart.max-file-size` | 20MB | Max file upload size |
| `spring.servlet.multipart.max-request-size` | 20MB | Max request size |

## Actuator

- `GET /actuator/health`
- `GET /actuator/info`

## OpenAPI

SpringDoc OpenAPI UI is available at:
- http://localhost:8080/swagger-ui.html
- http://localhost:8080/api-docs

## Docker Compose Services

| Service | Port | Purpose |
|---------|------|---------|
| postgres | 5432 | PostgreSQL database |
| rabbitmq | 5672, 15672 | RabbitMQ broker + Management UI |

RabbitMQ Management UI: http://localhost:15672 (guest / guest)

## Synchronous vs Asynchronous

### Synchronous approach (not used here)

```text
POST /api/files
      ↓
save file
      ↓
process file
      ↓
wait 3 seconds
      ↓
response
```

The client waits for processing.

### Asynchronous approach (this project)

```text
POST /api/files
      ↓
save file
      ↓
save metadata
      ↓
create job
      ↓
publish RabbitMQ message
      ↓
return immediately

RabbitMQ
      ↓
worker
      ↓
process file
```

This is the main concept the project teaches.

## License

MIT
