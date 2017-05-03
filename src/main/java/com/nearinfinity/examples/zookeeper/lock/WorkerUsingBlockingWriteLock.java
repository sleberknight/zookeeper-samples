package com.nearinfinity.examples.zookeeper.lock;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Worker uses the {@link BlockingWriteLock}.
 */
public class WorkerUsingBlockingWriteLock {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerUsingBlockingWriteLock.class);

    private WorkerUsingBlockingWriteLock() {
    }

    public static void main(String[] args) throws Exception {
        String hosts = args[0];
        String path = args[1];
        String myName = args[2];

        ConnectionHelper connectionHelper = new ConnectionHelper();
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);
        BlockingWriteLock lock = new BlockingWriteLock(myName, zooKeeper, path, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        LOG.info("{} is attempting to obtain lock on {}...", myName, path);

        lock.lock();

        LOG.info("{} has obtained lock on {}", myName, path);

        doSomeWork(myName);

        LOG.info("{} is done doing work, releasing lock on {}", myName, path);

        lock.unlock();  // Does not need to be in a finally. Why?  (hint: we're in a main method)
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
