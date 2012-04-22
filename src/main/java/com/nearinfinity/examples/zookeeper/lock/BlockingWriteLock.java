package com.nearinfinity.examples.zookeeper.lock;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;

public class BlockingWriteLock {

    private WriteLock writeLock;
    private static CountDownLatch lockAcquiredSignal = new CountDownLatch(1);

    public BlockingWriteLock(String name, ZooKeeper zookeeper, String dir, List<ACL> acl) {
        this.writeLock = new WriteLock(zookeeper, dir, acl, new SyncLockListener(name));
    }

    public void lock() throws InterruptedException, KeeperException {
        writeLock.lock();
        lockAcquiredSignal.await();
    }

    public void unlock() {
        writeLock.unlock();
    }

    static class SyncLockListener implements LockListener {

        private String name;

        SyncLockListener(String name) {
            this.name = name;
        }

        @Override
        public void lockAcquired() {
            System.out.printf("Lock acquired by %s\n", name);
            lockAcquiredSignal.countDown();
        }

        @Override
        public void lockReleased() {
            System.out.printf("Lock released by %s\n", name);
        }
    }
}
