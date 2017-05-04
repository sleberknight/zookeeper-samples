package com.nearinfinity.examples.zookeeper.lock;

import java.util.concurrent.CountDownLatch;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.WriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This worker uses the {@link WriteLock} and {@link LockListener} classes provided in ZooKeeper recipes.
 */
public class WorkerUsingWriteLockRecipe {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerUsingWriteLockRecipe.class);

    private static final CountDownLatch workDoneSignal = new CountDownLatch(1);

    private WorkerUsingWriteLockRecipe() {
    }

    public static void main(String[] args) throws Exception {
        String hosts = args[0];
        String path = args[1];
        String myName = args[2];

        ConnectionHelper connectionHelper = new ConnectionHelper();
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);
        WorkerLockListener lockListener = new WorkerLockListener(myName);
        WriteLock lock = new WriteLock(zooKeeper, path, ZooDefs.Ids.OPEN_ACL_UNSAFE, lockListener);
        LOG.info("{} trying to obtain lock on {}\n", myName, path);
        boolean gotLockImmediately = lock.lock();
        LOG.info("{} got lock immediately? {}", myName, gotLockImmediately);
        if (!gotLockImmediately) {
            LOG.info("{} waiting for lock...", myName);
        }
        workDoneSignal.await();

        LOG.info("Work done signal was sent. {} is unlocking the lock", myName);
        lock.unlock();  // Does not need to be in a finally. Why? (hint: we're in a main method)
    }

    static class WorkerLockListener implements LockListener {

        private final String workerName;

        WorkerLockListener(String workerName) {
            this.workerName = workerName;
        }

        @Override
        public void lockAcquired() {
            LOG.info("Lock acquired by {}", workerName);
            doSomeWork(workerName);
            LOG.info("{} is now done doing work", workerName);
            workDoneSignal.countDown();
        }

        @Override
        public void lockReleased() {
            LOG.info("Lock released by {}", workerName);
        }

        private static void doSomeWork(String name) {
            int seconds = new RandomAmountOfWork().timeItWillTake();
            long workTimeMillis = seconds * 1000L;
            LOG.info("{} is doing some work for {} seconds", name, seconds);
            try {
                Thread.sleep(workTimeMillis);
            } catch (InterruptedException ex) {
                LOG.error("Oops. Interrupted.", ex);
                Thread.currentThread().interrupt();
            }
        }
    }
}
