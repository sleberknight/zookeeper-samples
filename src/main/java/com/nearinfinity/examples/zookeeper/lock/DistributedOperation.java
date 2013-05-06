package com.nearinfinity.examples.zookeeper.lock;

public interface DistributedOperation<T> {
   T execute() throws DistributedOperationException;
}
