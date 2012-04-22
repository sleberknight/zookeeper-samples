package com.nearinfinity.examples.zookeeper.util;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

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
    private boolean manageDataDir;
    private int tickTime;
    private NIOServerCnxn.Factory cnxnFactory;

    public static final int DEFAULT_TICK_TIME = 2000;

    public EmbeddedZooKeeperServer(int port) {
        Random random = new Random(System.currentTimeMillis());
        this.dataDirectoryName = System.getProperty("java.io.tmpdir") + "zookeeper-data-" + random.nextInt();
        init(port, dataDirectoryName, true, DEFAULT_TICK_TIME);
    }

    public EmbeddedZooKeeperServer(int port, String dataDirectoryName, int tickTime) {
        init(port, dataDirectoryName, false, tickTime);
    }

    public void start() throws IOException, InterruptedException {
        ZooKeeperServer zkServer = new ZooKeeperServer();
        File dataDirectory = new File(dataDirectoryName);
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
        removeDataDirectoryIfNecessary(dataDirectoryName, manageDataDir);
    }

    private void init(int port, String dataDirectoryName, boolean manageDataDir, int tickTime) {
        this.port = port;
        this.dataDirectoryName = dataDirectoryName;
        this.manageDataDir = manageDataDir;
        createDataDirectoryIfNecessary(dataDirectoryName, manageDataDir);
        this.tickTime = tickTime;
    }

    private void createDataDirectoryIfNecessary(String dataDirectoryName, boolean manageDataDir) {
        if (manageDataDir) {
            File dataDir = new File(dataDirectoryName);
            if (!dataDir.exists()) {
                boolean dataDirCreated = dataDir.mkdirs();
                if (!dataDirCreated) {
                    throw new RuntimeException("Unable to create data directory: " + dataDirectoryName);
                }
            }
        }
    }

    private void removeDataDirectoryIfNecessary(String dataDirectoryName, boolean manageDataDir) {
        if (manageDataDir) {
            File dataDir = new File(dataDirectoryName);
            dataDir.deleteOnExit();  // TODO Why won't this (or normal delete() either) remove the directory on disk?
        }
    }

}
