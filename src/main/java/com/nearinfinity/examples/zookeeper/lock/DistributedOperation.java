package com.nearinfinity.examples.zookeeper.lock;

@FunctionalInterface
public interface DistributedOperation<T> {
    T execute() throws DistributedOperationException;
}
