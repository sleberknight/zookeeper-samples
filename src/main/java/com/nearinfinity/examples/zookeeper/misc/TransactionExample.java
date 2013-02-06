package com.nearinfinity.examples.zookeeper.misc;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The znode /txn-examples is the top level under which a new persistent sequential parent znodes will be created.
 * Under the /txn-examples/parent-NNNNNNNN znodes the specified children will be added using the Transaction wrapper
 * around ZooKeeper.multi() (both of which require ZK 3.4.0 or higher).
 */
public class TransactionExample {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        if (args.length < 4) {
            System.out.printf("Usage: %s <zk-connection-string> <parent-znode> <child-znode-1> <child-znode-2> [<child-znode-n> ...]\n",
                    TransactionExample.class.getSimpleName());
            System.exit(1);
        }

        ZooKeeper zooKeeper = new ConnectionHelper().connect(args[0]);

        String topZnodePath = "/txn-examples";
        if (zooKeeper.exists(topZnodePath, false) == null) {
            System.out.printf("Creating top level znode %s for transaction examples\n", topZnodePath);
            zooKeeper.create(topZnodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String baseParentPath = topZnodePath + "/" + args[1] + "-";
        String parentPath = zooKeeper.create(baseParentPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.printf("Created parent znode %s\n", parentPath);

        List<String> childPaths = new ArrayList<String>(args.length - 2);
        Transaction txn = zooKeeper.transaction();
        for (int i = 2; i < args.length; i++) {
            String childPath = parentPath + "/" + args[i];
            childPaths.add(childPath);
            System.out.printf("Adding create op with child path %s\n", childPath);
            txn.create(childPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println("Committing transaction");
        List<OpResult> opResults = txn.commit();

        System.out.println("Transactions results:");
        for (int i = 0; i < opResults.size(); i++) {
            OpResult opResult = opResults.get(i);
            int type = opResult.getType();
            String childPath = childPaths.get(i);
            switch (type) {
                case ZooDefs.OpCode.create:
                    System.out.printf("Child node %s created successfully\n", childPath);
                    break;
                case ZooDefs.OpCode.error:
                    System.out.printf("Child node %s was not created. There was an error.\n", childPath);
                    break;
                default:
                    System.out.printf("Don't know what happened with child node %s! OpResult type: %d", childPath, type);
            }
        }
    }
}