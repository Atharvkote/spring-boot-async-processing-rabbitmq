package net.spring_boot.rabbitmq.responses;

import java.time.Instant;

public class Response {
    private boolean success;
    private String message;
    private Object data;
    private Instant timestamp = Instant.now();

    public Response() {}

    public Response(boolean success, String message, Object data) {
        this.success = success;
        this.message = message;
        this.data = data;
    }

    public static Response success(String message, Object data) {
        return new Response(true, message, data);
    }

    public static Response success(Object data) {
        return new Response(true, "Success", data);
    }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Object getData() { return data; }
    public void setData(Object data) { this.data = data; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
