package com.nearinfinity.examples.zookeeper.lock;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingWriteLock {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingWriteLock.class);

    private final String name;
    private final String path;
    private final WriteLock writeLock;
    private final CountDownLatch lockAcquiredSignal = new CountDownLatch(1);

    private static final List<ACL> DEFAULT_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    public BlockingWriteLock(String name, ZooKeeper zookeeper, String path) {
        this(name, zookeeper, path, DEFAULT_ACL);
    }

    public BlockingWriteLock(String name, ZooKeeper zookeeper, String path, List<ACL> acl) {
        this.name = name;
        this.path = path;
        writeLock = new WriteLock(zookeeper, path, acl, new SyncLockListener());
    }

    public void lock() throws InterruptedException, KeeperException {
        LOG.debug("{} requesting lock on {}...", name, path);
        writeLock.lock();
        lockAcquiredSignal.await();
    }

    public boolean lock(long timeout, TimeUnit unit) throws InterruptedException, KeeperException {
        LOG.debug("{} requesting lock on {} with timeout {} {}...", name, path, timeout, unit);
        writeLock.lock();
        return lockAcquiredSignal.await(timeout, unit);
    }

    public boolean tryLock() throws InterruptedException, KeeperException {
        return lock(1, TimeUnit.SECONDS);
    }

    public void unlock() {
        writeLock.unlock();
    }

    class SyncLockListener implements LockListener {

        @Override
        public void lockAcquired() {
            LOG.debug("Lock acquired by {} on {}", name, path);
            lockAcquiredSignal.countDown();
        }

        @Override
        public void lockReleased() {
            LOG.debug("Lock released by {} on {}", name, path);
        }
    }
}
