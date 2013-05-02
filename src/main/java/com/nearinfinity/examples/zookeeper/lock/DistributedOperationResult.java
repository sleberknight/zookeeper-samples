package com.nearinfinity.examples.zookeeper.lock;

public class DistributedOperationResult {

    public final boolean timedOut;
    public final Object result;

    public DistributedOperationResult(boolean timedOut, Object result) {
        this.timedOut = timedOut;
        this.result = result;
    }
}
