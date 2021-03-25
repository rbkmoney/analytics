package com.rbkmoney.analytics.exception;

public class AdjustmentInfoNotFoundException extends RuntimeException {
    public AdjustmentInfoNotFoundException() {
    }

    public AdjustmentInfoNotFoundException(String message) {
        super(message);
    }

    public AdjustmentInfoNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public AdjustmentInfoNotFoundException(Throwable cause) {
        super(cause);
    }

    public AdjustmentInfoNotFoundException(String message, Throwable cause, boolean enableSuppression,
                                           boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
