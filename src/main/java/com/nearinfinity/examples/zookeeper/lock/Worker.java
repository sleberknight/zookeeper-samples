package com.nearinfinity.examples.zookeeper.lock;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;

public class Worker {

    private static Random random = new Random(System.currentTimeMillis());

    private static CountDownLatch workDoneSignal = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        String hosts = args[0];
        String path = args[1];
        String myName = args[2];

        ConnectionHelper connectionHelper = ConnectionHelper.instance;
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);
        WorkerLockListener lockListener = new WorkerLockListener(myName);
        WriteLock lock = new WriteLock(zooKeeper, path, ZooDefs.Ids.OPEN_ACL_UNSAFE, lockListener);
        System.out.printf("%s trying to obtain lock on %s...\n", myName, path);
        boolean gotLockImmediately = lock.lock();
        System.out.printf("%s got lock immediately? %b\n", myName, gotLockImmediately);
        if (!gotLockImmediately) {
            System.out.printf("%s waiting for lock...\n", myName);
        }
        workDoneSignal.await();

        System.out.printf("Work done signal was sent. %s is unlocking the lock\n", myName);
        lock.unlock();
    }

    private static void doSomeWork(String name) {
        int seconds = 10 + random.nextInt(10);  // sample work takes 10-20 seconds
        long workTimeMillis = seconds * 1000;
        System.out.printf("%s is doing some work for %d seconds\n", name, seconds);
        try {
            Thread.sleep(workTimeMillis);
        }
        catch (InterruptedException ex) {
            System.out.printf("Oops. Interrupted.\n");
            Thread.currentThread().interrupt();
        }
    }

    static class WorkerLockListener implements LockListener {

        private String workerName;

        WorkerLockListener(String workerName) {
            this.workerName = workerName;
        }

        @Override
        public void lockAcquired() {
            System.out.printf("Lock acquired by %s\n", workerName);
            doSomeWork(workerName);
            System.out.printf("%s is now done doing work\n", workerName);
            workDoneSignal.countDown();
        }

        @Override
        public void lockReleased() {
            System.out.printf("Lock released by %s\n", workerName);
        }
    }
}
