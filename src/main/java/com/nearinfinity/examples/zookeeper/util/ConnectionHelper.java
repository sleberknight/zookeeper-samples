package com.nearinfinity.examples.zookeeper.util;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ConnectionHelper {

    public static final int SESSION_TIMEOUT = 5000;

    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public static final ConnectionHelper instance;

    static {
        instance = new ConnectionHelper();
    }

    public ZooKeeper connect(String hosts, int sessionTimeout) throws IOException, InterruptedException {
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
        return connect(hosts, SESSION_TIMEOUT);
    }
}
