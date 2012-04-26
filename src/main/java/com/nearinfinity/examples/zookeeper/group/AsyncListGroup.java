package com.nearinfinity.examples.zookeeper.group;

import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;

import com.nearinfinity.examples.zookeeper.util.ConnectionWatcher;

public class AsyncListGroup extends ConnectionWatcher {

    public void list(final String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;

        // In real code, you would not use the async API the way it's being used here. You would
        // go off and do other things without blocking like this example does.
        final Semaphore semaphore = new Semaphore(1);
        zk.getChildren(path, false,
                new AsyncCallback.ChildrenCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> children) {
                        System.out.printf("Called back for path %s with return code %d\n", path, rc);
                        if (children == null) {
                            System.out.printf("Group %s does not exist\n", groupName);
                        }
                        else {
                            if (children.isEmpty()) {
                                System.out.printf("No members in group %s\n", groupName);
                                return;
                            }
                            for (String child : children) {
                                System.out.println(child);
                            }
                        }
                        semaphore.release();
                    }
                }, null /* optional context object */);
        semaphore.acquire();
    }

    public static void main(String[] args) throws Exception {
        AsyncListGroup asyncListGroup = new AsyncListGroup();
        asyncListGroup.connect(args[0]);
        asyncListGroup.list(args[1]);
        asyncListGroup.close();
    }

}
