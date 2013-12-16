package com.nearinfinity.examples.zookeeper.lock;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;

/**
 * This Worker uses the {@link BlockingWriteLock}.
 */
public class WorkerUsingBlockingWriteLock {

    public static void main(String[] args) throws Exception {
        String hosts = args[0];
        String path = args[1];
        String myName = args[2];

        ConnectionHelper connectionHelper = new ConnectionHelper();
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);
        BlockingWriteLock lock = new BlockingWriteLock(myName, zooKeeper, path, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        System.out.printf("%s is attempting to obtain lock on %s...\n", myName, path);

        lock.lock();

        System.out.printf("%s has obtained lock on %s\n", myName, path);

        doSomeWork(myName);

        System.out.printf("%s is done doing work, releasing lock on %s\n", myName, path);

        lock.unlock();  // Does not need to be in a finally. Why?  (hint: we're in a main method)
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
}
