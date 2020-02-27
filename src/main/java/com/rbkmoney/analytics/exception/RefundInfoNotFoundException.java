package com.rbkmoney.analytics.exception;

public class RefundInfoNotFoundException extends RuntimeException {
    public RefundInfoNotFoundException() {
    }

    public RefundInfoNotFoundException(String message) {
        super(message);
    }

    public RefundInfoNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public RefundInfoNotFoundException(Throwable cause) {
        super(cause);
    }

    public RefundInfoNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
