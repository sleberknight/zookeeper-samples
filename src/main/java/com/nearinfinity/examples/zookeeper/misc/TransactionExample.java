package com.nearinfinity.examples.zookeeper.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.MoreZKPaths;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The znode /txn-examples is the top level under which a new persistent sequential parent znodes will be created.
 * Under the /txn-examples/parent-NNNNNNNN znodes the specified children will be added using the Transaction wrapper
 * around ZooKeeper.multi() (both of which require ZK 3.4.0 or higher).
 */
public class TransactionExample {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionExample.class);

    private TransactionExample() {
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        if (args.length < 4) {
            LOG.info("Usage: {} <zk-connection-string> <parent-znode> <child-znode-1> <child-znode-2> [<child-znode-n> ...]",
                    TransactionExample.class.getSimpleName());
            System.exit(1);
        }

        ZooKeeper zooKeeper = new ConnectionHelper().connect(args[0]);

        String topZnodePath = MoreZKPaths.makeAbsolutePath("txn-examples");
        if (zooKeeper.exists(topZnodePath, false) == null) {
            LOG.info("Creating top level znode {} for transaction examples", topZnodePath);
            zooKeeper.create(topZnodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String baseParentPath = ZKPaths.makePath(topZnodePath, args[1] + "-");
        String parentPath = zooKeeper.create(baseParentPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        LOG.info("Created parent znode {}", parentPath);

        List<String> childPaths = new ArrayList<>(args.length - 2);
        Transaction txn = zooKeeper.transaction();
        for (int i = 2; i < args.length; i++) {
            String childPath = ZKPaths.makePath(parentPath, args[i]);
            childPaths.add(childPath);
            LOG.info("Adding create op with child path {}", childPath);
            txn.create(childPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        LOG.info("Committing transaction");
        List<OpResult> opResults = txn.commit();

        LOG.info("Transactions results:");
        for (int i = 0; i < opResults.size(); i++) {
            OpResult opResult = opResults.get(i);
            int type = opResult.getType();
            String childPath = childPaths.get(i);
            switch (type) {
                case ZooDefs.OpCode.create:
                    LOG.info("Child node {} created successfully", childPath);
                    break;
                case ZooDefs.OpCode.error:
                    LOG.info("Child node {} was not created. There was an error.", childPath);
                    break;
                default:
                    LOG.info("Don't know what happened with child node {}! OpResult type: {}", childPath, type);
            }
        }
    }
}