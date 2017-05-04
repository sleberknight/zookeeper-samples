package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockWatcher implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(LockWatcher.class);

    private final ZooKeeper zk;
    private final String lockPath;
    private final Semaphore semaphore = new Semaphore(1);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String hosts = args[0];
        String lockPath = args[1];

        ConnectionHelper connectionHelper = new ConnectionHelper();
        ZooKeeper zk = connectionHelper.connect(hosts);

        LockWatcher watcher = new LockWatcher(zk, lockPath);
        watcher.watch();
    }

    public LockWatcher(ZooKeeper zk, String lockPath) throws InterruptedException, KeeperException {
        this.zk = zk;
        this.lockPath = lockPath;
        ensureLockPathExists(zk, lockPath);
    }

    @SuppressWarnings("squid:S2189")
    public void watch() throws InterruptedException, KeeperException {
        LOG.info("Acquire initial semaphore");
        semaphore.acquire();

        // noinspection InfiniteLoopStatement
        while (true) {
            LOG.info("Getting children for lock path {}", lockPath);
            List<String> children = zk.getChildren(lockPath, this);
            printChildren(children);
            semaphore.acquire();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            LOG.info("Received {} event", Event.EventType.NodeChildrenChanged);
            semaphore.release();
        }
    }

    private void ensureLockPathExists(ZooKeeper zk, String lockPath)
            throws InterruptedException, KeeperException {

        if (zk.exists(lockPath, false) == null) {
            String znodePath =
                    zk.create(lockPath, null /* data */, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info("Created lock znode having path {}", znodePath);
        }
    }

    private void printChildren(List<String> children) {
        if (children.isEmpty()) {
            LOG.info("No one has the lock on {} at the moment...", lockPath);
            return;
        }

        LOG.info("Current lock nodes at {}:", new Date());
        for (String child : children) {
            LOG.info("  {}", child);
        }
        LOG.info("--------------------");
    }
}
