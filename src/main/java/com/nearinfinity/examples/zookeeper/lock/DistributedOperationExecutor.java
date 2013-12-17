package com.nearinfinity.examples.zookeeper.lock;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public class DistributedOperationExecutor {

    private ZooKeeper _zk;

    public DistributedOperationExecutor(ZooKeeper zk) {
        _zk = zk;
    }

    public static final List<ACL> DEFAULT_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    public <T> T withLock(String name, String lockPath, DistributedOperation<T> op)
            throws InterruptedException, KeeperException {
        return withLockInternal(name, lockPath, DEFAULT_ACL, op);
    }

    public <T> DistributedOperationResult<T> withLock(String name, String lockPath, DistributedOperation<T> op,
                                                      long timeout, TimeUnit unit)
            throws InterruptedException, KeeperException {
        return withLockInternal(name, lockPath, DEFAULT_ACL, op, timeout, unit);
    }

    public <T> T withLock(String name, String lockPath, List<ACL> acl, DistributedOperation<T> op)
            throws InterruptedException, KeeperException {
        return withLockInternal(name, lockPath, acl, op);
    }

    public <T> DistributedOperationResult<T> withLock(String name, String lockPath, List<ACL> acl, DistributedOperation<T> op,
                                                      long timeout, TimeUnit unit)
            throws InterruptedException, KeeperException {
        return withLockInternal(name, lockPath, acl, op, timeout, unit);
    }

    private <T> T withLockInternal(String name, String lockPath, List<ACL> acl, DistributedOperation<T> op)
            throws InterruptedException, KeeperException {
        BlockingWriteLock lock = new BlockingWriteLock(name, _zk, lockPath, acl);
        try {
            lock.lock();
            return op.execute();
        } finally {
            lock.unlock();
        }
    }

    private <T> DistributedOperationResult<T> withLockInternal(String name, String lockPath, List<ACL> acl,
                                                               DistributedOperation<T> op, long timeout, TimeUnit unit)
            throws InterruptedException, KeeperException {
        BlockingWriteLock lock = new BlockingWriteLock(name, _zk, lockPath, acl);
        try {
            boolean lockObtained = lock.lock(timeout, unit);
            if (lockObtained) {
                return new DistributedOperationResult<T>(false, op.execute());
            }
            return new DistributedOperationResult<T>(true, null);
        } finally {
            lock.unlock();
        }
    }

}
