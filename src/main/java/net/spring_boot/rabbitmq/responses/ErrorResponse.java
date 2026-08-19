package net.spring_boot.rabbitmq.responses;

import java.time.Instant;

public class ErrorResponse {
    private String code;
    private String message;
    private Instant timestamp = Instant.now();

    public ErrorResponse() {}

    public ErrorResponse(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
