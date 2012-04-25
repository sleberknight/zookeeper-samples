package com.nearinfinity.examples.zookeeper.misc;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ConnectingExample {

    private static final int SESSION_TIMEOUT = 5000;

    public ZooKeeper connect(String hosts) throws IOException, InterruptedException {
        final CountDownLatch signal = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    signal.countDown();
                }
            }
        });
        signal.await();
        return zk;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectingExample example = new ConnectingExample();
        ZooKeeper zk = example.connect("localhost:2181");
        System.out.printf("ZK state: %s\n", zk.getState());
        zk.close();
    }
}
