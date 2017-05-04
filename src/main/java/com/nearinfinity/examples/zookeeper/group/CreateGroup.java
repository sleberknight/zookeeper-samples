package com.nearinfinity.examples.zookeeper.group;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.nearinfinity.examples.zookeeper.util.MoreZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateGroup implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(CreateGroup.class);

    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zk;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public void connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    @Override
    public void process(WatchedEvent event) { // Watcher interface
        if (event.getState() == Event.KeeperState.SyncConnected) {
            LOG.info("Connected...");
            connectedSignal.countDown();
        }
    }

    public void create(String groupName) throws KeeperException, InterruptedException {
        String path = MoreZKPaths.makeAbsolutePath(groupName);
        String createdPath = zk.create(path,
                null /*data*/,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        LOG.info("Created {}", createdPath);
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public static void main(String[] args) throws Exception {
        CreateGroup createGroup = new CreateGroup();
        createGroup.connect(args[0]);
        createGroup.create(args[1]);
        createGroup.close();
    }

}
