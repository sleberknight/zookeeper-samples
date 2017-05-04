package com.nearinfinity.examples.zookeeper.group;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupMembershipIterable implements Iterable<List<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupMembershipIterable.class);

    private final ZooKeeper zooKeeper;
    private final String groupName;
    private final String groupPath;
    private final Semaphore semaphore = new Semaphore(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeper zk = new ConnectionHelper().connect(args[0]);
        String theGroupName = args[1];
        GroupMembershipIterable iterable = new GroupMembershipIterable(zk, theGroupName);
        Iterator<List<String>> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            LOG.info("{}", iterator.next());
            LOG.info("--------------------");
        }
        LOG.info("Group {} does not exist (anymore)!", theGroupName);
    }

    public GroupMembershipIterable(ZooKeeper zooKeeper, String groupName) {
        this.zooKeeper = zooKeeper;
        this.groupName = groupName;
        groupPath = pathFor(groupName);
    }

    @Override
    public Iterator<List<String>> iterator() {
        return new Iterator<List<String>>() {

            /**
             * Attempts to acquire the semaphore. Once acquired, returns true if the group node still exists, otherwise
             * false is the group znode has been deleted.
             */
            @Override
            public boolean hasNext() {
                try {
                    semaphore.acquire();
                    return zooKeeper.exists(groupPath, false) != null;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }

            /**
             * Lists the group contents, setting a watch. When either the node children change, or the group node
             * is deleted, the semaphore acquired in {@link #hasNext()} is released.
             *
             * @implNote Suppressed Sonar warning: "Iterator.next()" methods should throw "NoSuchElementException"
             * because with this implementation (which maybe is not a good design...) if hasNext() is called, it acquires
             * the semaphore, then if next() is called and it calls hasNext() again, then it will block trying to
             * acquire the semaphore - in fact it will block indefinitely and never acquire it, because the code will
             * never get into the list() method where the release occurs.
             */
            @SuppressWarnings("squid:S2272")
            @Override
            public List<String> next() {
                try {
                    return list(groupName);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Removing znodes not supported");
            }
        };
    }

    private List<String> list(final String groupName) throws KeeperException, InterruptedException {
        String path = pathFor(groupName);
        List<String> children = zooKeeper.getChildren(path, event -> {
            if (isNodeChildrenChangedEvent(event) || isNodeDeletedEventForGroup(event)) {
                semaphore.release();
            }
        });
        Collections.sort(children);
        return children;
    }

    private boolean isNodeChildrenChangedEvent(WatchedEvent event) {
        return event.getType() == Watcher.Event.EventType.NodeChildrenChanged;
    }

    private boolean isNodeDeletedEventForGroup(WatchedEvent event) {
        return event.getType() == Watcher.Event.EventType.NodeDeleted && event.getPath().equals(groupPath);
    }

    private String pathFor(String groupName) {
        return "/" + groupName;
    }

}
