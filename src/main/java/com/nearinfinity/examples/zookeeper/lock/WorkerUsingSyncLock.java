package com.nearinfinity.examples.zookeeper.lock;

import java.util.Random;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;

public class WorkerUsingSyncLock {

    private static Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) throws Exception {
        String hosts = args[0];
        String path = args[1];
        String myName = args[2];

        ConnectionHelper connectionHelper = ConnectionHelper.instance;
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);
        SynchronousWriteLock lock = new SynchronousWriteLock(myName, zooKeeper, path, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        System.out.printf("%s is attempting to obtain lock on %s...\n", myName, path);

        lock.lock();

        System.out.printf("%s has obtained lock on %s\n", myName, path);

        doSomeWork(myName);

        System.out.printf("%s is done doing work, releasing lock on %s", myName, path);

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
}
