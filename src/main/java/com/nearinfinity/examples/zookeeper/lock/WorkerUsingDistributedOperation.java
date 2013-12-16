package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;

/**
 * This Worker uses the {@link DistributedOperationExecutor} (which currently uses {@link BlockingWriteLock}
 * internally).
 */
public class WorkerUsingDistributedOperation {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        final String hosts = args[0];
        final String path = args[1];
        final String myName = args[2];

        ConnectionHelper connectionHelper = new ConnectionHelper();
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);

        new DistributedOperationExecutor(zooKeeper).withLock(myName, path, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                new DistributedOperation<Void>() {
                    @Override
                    public Void execute() {
                        int seconds = new RandomAmountOfWork().timeItWillTake();
                        long workTimeMillis = seconds * 1000;
                        System.out.printf("%s is doing some work for %d seconds\n", myName, seconds);
                        try {
                            Thread.sleep(workTimeMillis);
                        }
                        catch (InterruptedException ex) {
                            System.out.printf("Oops. Interrupted.\n");
                            Thread.currentThread().interrupt();
                        }
                        return null;
                    }
                }
        );
    }

}
