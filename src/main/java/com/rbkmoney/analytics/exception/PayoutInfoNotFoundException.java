package com.rbkmoney.analytics.exception;

public class PayoutInfoNotFoundException extends RuntimeException {

    public PayoutInfoNotFoundException(String message) {
        super(message);
    }

    public PayoutInfoNotFoundException(Throwable cause) {
        super(cause);
    }
}