package com.nearinfinity.examples.zookeeper.lock;

import java.util.concurrent.TimeUnit;

import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * This Worker uses the Curator {@link InterProcessMutex} class to perform locking.
 */
public class WorkerUsingCurator {

    static final long DEFAULT_WAIT_TIME_SECONDS = Long.MAX_VALUE;

    public static void main(String[] args) throws Exception {
        String hosts = args[0];
        String lockPath = args[1];
        String myName = args[2];

        long waitTimeSeconds = DEFAULT_WAIT_TIME_SECONDS;
        if (args.length == 4) {
            waitTimeSeconds = Long.valueOf(args[3]);
        }

        int baseSleepTimeMills = 1000;
        int maxRetries = 3;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMills, maxRetries);
        CuratorFramework client = CuratorFrameworkFactory.newClient(hosts, retryPolicy);
        client.start();

        InterProcessLock lock = new InterProcessMutex(client, lockPath);
        if (lock.acquire(waitTimeSeconds, TimeUnit.SECONDS)) {
            try {
                doSomeWork(myName);
            } finally {
                lock.release();
            }
        } else {
            System.err.printf("%s timed out after %d seconds waiting to acquire lock on %s\n",
                    myName, waitTimeSeconds, lockPath);
        }

        client.close();
    }

    private static void doSomeWork(String name) {
        int seconds = new RandomAmountOfWork().timeItWillTake();
        long workTimeMillis = seconds * 1000;
        System.out.printf("%s is doing some work for %d seconds\n", name, seconds);
        try {
            Thread.sleep(workTimeMillis);
        } catch (InterruptedException ex) {
            System.out.printf("Oops. Interrupted.\n");
            Thread.currentThread().interrupt();
        }
    }

}
