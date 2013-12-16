package com.nearinfinity.examples.zookeeper.lock;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;

/**
 * This worker uses the {@link WriteLock} and {@link LockListener} classes provided in ZooKeeper recipes.
 */
public class WorkerUsingWriteLockRecipe {

    private static CountDownLatch _workDoneSignal = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        String hosts = args[0];
        String path = args[1];
        String myName = args[2];

        ConnectionHelper connectionHelper = new ConnectionHelper();
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);
        WorkerLockListener lockListener = new WorkerLockListener(myName);
        WriteLock lock = new WriteLock(zooKeeper, path, ZooDefs.Ids.OPEN_ACL_UNSAFE, lockListener);
        System.out.printf("%s trying to obtain lock on %s...\n", myName, path);
        boolean gotLockImmediately = lock.lock();
        System.out.printf("%s got lock immediately? %b\n", myName, gotLockImmediately);
        if (!gotLockImmediately) {
            System.out.printf("%s waiting for lock...\n", myName);
        }
        _workDoneSignal.await();

        System.out.printf("Work done signal was sent. %s is unlocking the lock\n", myName);
        lock.unlock();  // Does not need to be in a finally. Why? (hint: we're in a main method)
    }

    private static void doSomeWork(String name) {
        int seconds = new RandomAmountOfWork().timeItWillTake();
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

        private String _workerName;

        WorkerLockListener(String workerName) {
            _workerName = workerName;
        }

        @Override
        public void lockAcquired() {
            System.out.printf("Lock acquired by %s\n", _workerName);
            doSomeWork(_workerName);
            System.out.printf("%s is now done doing work\n", _workerName);
            _workDoneSignal.countDown();
        }

        @Override
        public void lockReleased() {
            System.out.printf("Lock released by %s\n", _workerName);
        }
    }
}
