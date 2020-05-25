package com.rbkmoney.analytics.exception;

public class ChargebackInfoNotFoundException extends RuntimeException {
    public ChargebackInfoNotFoundException() {
    }

    public ChargebackInfoNotFoundException(String message) {
        super(message);
    }

    public ChargebackInfoNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ChargebackInfoNotFoundException(Throwable cause) {
        super(cause);
    }

    public ChargebackInfoNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
