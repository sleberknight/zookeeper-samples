package com.nearinfinity.examples.zookeeper.util;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ConnectionHelper {

    public static final int DEFAULT_SESSION_TIMEOUT = 5000;

    public ZooKeeper connect(String hosts, int sessionTimeout) throws IOException, InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(hosts, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        connectedSignal.await();
        return zk;
    }

    public ZooKeeper connect(String hosts) throws IOException, InterruptedException {
        return connect(hosts, DEFAULT_SESSION_TIMEOUT);
    }
}
