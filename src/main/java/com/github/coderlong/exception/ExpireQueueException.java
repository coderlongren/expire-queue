package com.github.coderlong.exception;

public class ExpireQueueException extends Exception{
    public ExpireQueueException() {
    }

    public ExpireQueueException(String message) {
        super(message);
    }

    public ExpireQueueException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExpireQueueException(Throwable cause) {
        super(cause);
    }

    public ExpireQueueException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
