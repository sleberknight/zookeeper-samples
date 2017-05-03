package com.nearinfinity.examples.zookeeper.group;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListGroupForever {

    private static final Logger LOG = LoggerFactory.getLogger(ListGroupForever.class);

    private ZooKeeper _zooKeeper;
    private Semaphore _semaphore = new Semaphore(1);

    public ListGroupForever(ZooKeeper zooKeeper) {
        _zooKeeper = zooKeeper;
    }

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ConnectionHelper().connect(args[0]);
        new ListGroupForever(zk).listForever(args[1]);
    }

    @SuppressWarnings("squid:S2189")
    public void listForever(String groupName) throws KeeperException, InterruptedException {
        _semaphore.acquire();
        while (true) {
            list(groupName);
            _semaphore.acquire();
        }
    }

    private void list(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;

        List<String> children = _zooKeeper.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    _semaphore.release();
                }
            }
        });
        if (children.isEmpty()) {
            LOG.info("No members in group {}", groupName);
            return;
        }
        Collections.sort(children);
        LOG.info("{}", children);
        LOG.info("--------------------");
    }
}
