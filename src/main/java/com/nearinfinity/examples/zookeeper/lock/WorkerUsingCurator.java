package com.nearinfinity.examples.zookeeper.lock;

import java.util.concurrent.TimeUnit;

import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Worker uses the Curator {@link InterProcessMutex} class to perform locking.
 */
public class WorkerUsingCurator {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerUsingCurator.class);

    private static final long DEFAULT_WAIT_TIME_SECONDS = Long.MAX_VALUE;

    private WorkerUsingCurator() {
    }

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
            LOG.error("{} timed out after {} seconds waiting to acquire lock on {}",
                    myName, waitTimeSeconds, lockPath);
        }

        client.close();
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
