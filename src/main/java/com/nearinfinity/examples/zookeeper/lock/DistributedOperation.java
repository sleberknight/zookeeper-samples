package com.nearinfinity.examples.zookeeper.lock;

public interface DistributedOperation {
   Object execute() throws DistributedOperationException;
}
