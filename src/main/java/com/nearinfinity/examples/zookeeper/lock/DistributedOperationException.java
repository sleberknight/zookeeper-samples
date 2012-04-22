package com.nearinfinity.examples.zookeeper.lock;

public class DistributedOperationException extends RuntimeException {
    public DistributedOperationException() {
    }

    public DistributedOperationException(String message) {
        super(message);
    }

    public DistributedOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DistributedOperationException(Throwable cause) {
        super(cause);
    }
}
