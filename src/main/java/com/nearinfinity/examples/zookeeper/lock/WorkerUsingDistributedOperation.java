package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Worker uses the {@link DistributedOperationExecutor} (which currently uses {@link BlockingWriteLock}
 * internally).
 */
public class WorkerUsingDistributedOperation {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerUsingDistributedOperation.class);

    private WorkerUsingDistributedOperation() {
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        final String hosts = args[0];
        final String path = args[1];
        final String myName = args[2];

        ConnectionHelper connectionHelper = new ConnectionHelper();
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);

        new DistributedOperationExecutor(zooKeeper).withLock(myName, path, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                () -> {
                    int seconds = new RandomAmountOfWork().timeItWillTake();
                    long workTimeMillis = seconds * 1000L;
                    LOG.info("{} is doing some work for {} seconds", myName, seconds);
                    try {
                        Thread.sleep(workTimeMillis);
                    } catch (InterruptedException ex) {
                        LOG.error("Oops. Interrupted.", ex);
                        Thread.currentThread().interrupt();
                    }
                    return null;
                }
        );
    }

}
