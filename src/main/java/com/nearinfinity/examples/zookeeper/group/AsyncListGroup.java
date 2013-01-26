package com.nearinfinity.examples.zookeeper.group;

import com.nearinfinity.examples.zookeeper.util.ConnectionWatcher;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class AsyncListGroup extends ConnectionWatcher {

    public static void main(String[] args) throws Exception {
        AsyncListGroup asyncListGroup = new AsyncListGroup();
        asyncListGroup.connect(args[0]);
        asyncListGroup.list(args[1]);
        asyncListGroup.close();
    }

    public void list(final String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;

        // In real code, you would not use the async API the way it's being used here. You would
        // go off and do other things without blocking like this example does.
        final CountDownLatch latch = new CountDownLatch(1);
        zk.getChildren(path, false,
                new AsyncCallback.ChildrenCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> children) {
                        System.out.printf("Called back for path %s with return code %d\n", path, rc);
                        if (children == null) {
                            System.out.printf("Group %s does not exist\n", groupName);
                        } else {
                            if (children.isEmpty()) {
                                System.out.printf("No members in group %s\n", groupName);
                                return;
                            }
                            for (String child : children) {
                                System.out.println(child);
                            }
                        }
                        latch.countDown();
                    }
                }, null /* optional context object */);
        System.out.println("Awaiting latch countdown...");
        latch.await();
    }

}
