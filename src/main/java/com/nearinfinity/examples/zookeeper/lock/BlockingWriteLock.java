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

public class BlockingWriteLock {

    private String name;
    private String path;
    private WriteLock writeLock;
    private CountDownLatch lockAcquiredSignal = new CountDownLatch(1);

    public static final List<ACL> DEFAULT_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    public BlockingWriteLock(String name, ZooKeeper zookeeper, String path) {
        this(name, zookeeper, path, DEFAULT_ACL);
    }

    public BlockingWriteLock(String name, ZooKeeper zookeeper, String path, List<ACL> acl) {
        this.name = name;
        this.path = path;
        this.writeLock = new WriteLock(zookeeper, path, acl, new SyncLockListener());
    }

    public void lock() throws InterruptedException, KeeperException {
        System.out.printf("%s requesting lock on %s...\n", name, path);
        writeLock.lock();
        lockAcquiredSignal.await();
    }

    public boolean lock(long timeout, TimeUnit unit) throws InterruptedException, KeeperException {
        System.out.printf("%s requesting lock on %s with timeout %d %s...\n", name, path, timeout, unit.name());
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
            System.out.printf("Lock acquired by %s on %s\n", name, path);
            lockAcquiredSignal.countDown();
        }

        @Override
        public void lockReleased() {
            System.out.printf("Lock released by %s on %s\n", name, path);
        }
    }
}
