package com.nearinfinity.examples.zookeeper.group;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

public class GroupMembershipIterable implements Iterable<List<String>> {

    private ZooKeeper zooKeeper;
    private String groupName;
    private String groupPath;
    private Semaphore semaphore = new Semaphore(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeper zk = new ConnectionHelper().connect(args[0]);
        String theGroupName = args[1];
        GroupMembershipIterable iterable = new GroupMembershipIterable(zk, theGroupName);
        Iterator<List<String>> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println("--------------------");
        }
        System.out.printf("Group %s does not exist (anymore)!", theGroupName);
    }

    public GroupMembershipIterable(ZooKeeper zooKeeper, String groupName) {
        this.zooKeeper = zooKeeper;
        this.groupName = groupName;
        this.groupPath = pathFor(groupName);
    }

    @Override
    public Iterator<List<String>> iterator() {
        return new Iterator<List<String>>() {
            @Override
            public boolean hasNext() {
                try {
                    semaphore.acquire();
                    return zooKeeper.exists(groupPath, false) != null;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public List<String> next() {
                try {
                    return list(groupName);
                } catch (InterruptedException e) {
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
        List<String> children = zooKeeper.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    semaphore.release();
                } else if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(groupPath)) {
                    semaphore.release();
                }
            }
        });
        Collections.sort(children);
        return children;
    }

    private String pathFor(String groupName) {
        return "/" + groupName;
    }

}
