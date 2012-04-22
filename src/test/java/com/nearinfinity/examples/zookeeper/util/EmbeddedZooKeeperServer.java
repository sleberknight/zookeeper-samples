package com.nearinfinity.examples.zookeeper.util;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * Mimics what {@link org.apache.zookeeper.server.ZooKeeperServerMain} does, but provides a way to shut it down for
 * unit/integration testing purposes.
 */
public final class EmbeddedZooKeeperServer {

    private int port;
    private String dataDirectoryName;
    private int tickTime;
    ZooKeeperServer zkServer;
    NIOServerCnxn.Factory cnxnFactory;

    public EmbeddedZooKeeperServer(int port, String dataDirectoryName, int tickTime) {
        this.port = port;
        this.dataDirectoryName = dataDirectoryName;
        this.tickTime = tickTime;
    }

    public void start() throws IOException, InterruptedException {
        File dataDirectory = new File(dataDirectoryName);
        zkServer = new ZooKeeperServer();
        FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDirectory, dataDirectory);
        zkServer.setTxnLogFactory(ftxn);
        zkServer.setTickTime(tickTime);
        cnxnFactory = new NIOServerCnxn.Factory(new InetSocketAddress(port));
        cnxnFactory.startup(zkServer);
    }

    public void shutdown() {
        if (cnxnFactory != null && cnxnFactory.isAlive()) {
            cnxnFactory.shutdown();
        }
    }

}
