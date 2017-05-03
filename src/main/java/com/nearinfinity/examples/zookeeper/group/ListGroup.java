package com.nearinfinity.examples.zookeeper.group;

import java.util.List;

import com.nearinfinity.examples.zookeeper.util.ConnectionWatcher;
import com.nearinfinity.examples.zookeeper.util.MoreZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListGroup extends ConnectionWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ListGroup.class);

    public void list(String groupName) throws KeeperException, InterruptedException {
        String path = MoreZKPaths.makeAbsolutePath(groupName);

        try {
            List<String> children = zk.getChildren(path, false);
            if (children.isEmpty()) {
                LOG.info("No members in group {}", groupName);
                return;
            }
            for (String child : children) {
                LOG.info(child);
            }
        } catch (KeeperException.NoNodeException e) {
            LOG.error("Group {} does not exist", groupName, e);
        }
    }

    public static void main(String[] args) throws Exception {
        ListGroup listGroup = new ListGroup();
        listGroup.connect(args[0]);
        listGroup.list(args[1]);
        listGroup.close();
    }


}
