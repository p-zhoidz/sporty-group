package com.sportygroup.settlement.expansion.messaging;

public class MalformedKafkaMessageException extends RuntimeException {
    public MalformedKafkaMessageException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedKafkaMessageException(String message) {
        super(message);
    }
}
