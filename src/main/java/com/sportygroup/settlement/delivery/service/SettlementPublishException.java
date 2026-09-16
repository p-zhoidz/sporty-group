package com.sportygroup.settlement.delivery.service;

public class SettlementPublishException extends RuntimeException {

    public SettlementPublishException(String message) {
        super(message);
    }

    public SettlementPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
