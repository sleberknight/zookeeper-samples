package com.nearinfinity.examples.zookeeper.group;

import java.util.List;

import com.nearinfinity.examples.zookeeper.util.ConnectionWatcher;
import com.nearinfinity.examples.zookeeper.util.MoreZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteGroup extends ConnectionWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteGroup.class);

    public void delete(String groupName) throws KeeperException, InterruptedException {
        String path = MoreZKPaths.makeAbsolutePath(groupName);

        try {
            List<String> children = zk.getChildren(path, false);
            for (String child : children) {
                zk.delete(path + "/" + child, -1);
            }
            zk.delete(path, -1);
            LOG.info("Deleted group {} at path {}", groupName, path);
        } catch (KeeperException.NoNodeException e) {
            LOG.error("Group {} does not exist", groupName, e);
        }
    }


    public static void main(String[] args) throws Exception {
        DeleteGroup deleteGroup = new DeleteGroup();
        deleteGroup.connect(args[0]);
        deleteGroup.delete(args[1]);
        deleteGroup.close();
    }

}
