package com.nearinfinity.examples.zookeeper.group;

import com.nearinfinity.examples.zookeeper.util.ConnectionWatcher;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinGroup extends ConnectionWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(JoinGroup.class);

    public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
        String path = ZKPaths.makePath(groupName, memberName);
        String createdPath = zk.create(path,
                null/*data*/,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        LOG.info("Created {}", createdPath);
    }

    public static void main(String[] args) throws Exception {
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect(args[0]);
        joinGroup.join(args[1], args[2]);

        // stay alive until process is killed or thread is interrupted
        Thread.sleep(Long.MAX_VALUE);
    }

}
