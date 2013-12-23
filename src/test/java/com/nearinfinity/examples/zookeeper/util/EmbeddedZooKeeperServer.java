package com.nearinfinity.examples.zookeeper.util;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * Mimics what {@link org.apache.zookeeper.server.ZooKeeperServerMain} does, but provides a way to shut it down for
 * unit/integration testing purposes.
 *
 * TODO After upgrading to ZK 3.4.5 the tests execute slower, sometimes much slower and it looks like, from the logs,
 *      there are a bunch of issues related to disconnects and reconnect attempts. Not sure why.
 * 
 * TODO Better idea: replace this with Curator's TestingServer.
 */
public final class EmbeddedZooKeeperServer {

    private int _port;
    private String _dataDirectoryName;
    private boolean _manageDataDir;
    private int _tickTime;
    private NIOServerCnxnFactory _cnxnFactory;

    public static final int MAX_CLIENT_CONNECTIONS = 60;
    public static final int DEFAULT_TICK_TIME = 2000;

    public EmbeddedZooKeeperServer(int port) {
        Random random = new Random(System.currentTimeMillis());
        this._dataDirectoryName = System.getProperty("java.io.tmpdir") + "zookeeper-data-" + random.nextInt();
        init(port, _dataDirectoryName, true, DEFAULT_TICK_TIME);
    }

    public EmbeddedZooKeeperServer(int port, String dataDirectoryName, int tickTime) {
        init(port, dataDirectoryName, false, tickTime);
    }

    public void start() throws IOException, InterruptedException {
        ZooKeeperServer zkServer = new ZooKeeperServer();
        File dataDirectory = new File(_dataDirectoryName);
        FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDirectory, dataDirectory);
        zkServer.setTxnLogFactory(ftxn);
        zkServer.setTickTime(_tickTime);
        _cnxnFactory = new NIOServerCnxnFactory();
        _cnxnFactory.configure(new InetSocketAddress(_port), MAX_CLIENT_CONNECTIONS);
        _cnxnFactory.startup(zkServer);
    }

    public void shutdown() {
        if (_cnxnFactory != null) {
            _cnxnFactory.shutdown();
        }
        removeDataDirectoryIfNecessary(_dataDirectoryName, _manageDataDir);
    }

    private void init(int port, String dataDirectoryName, boolean manageDataDir, int tickTime) {
        this._port = port;
        this._dataDirectoryName = dataDirectoryName;
        this._manageDataDir = manageDataDir;
        createDataDirectoryIfNecessary(dataDirectoryName, manageDataDir);
        this._tickTime = tickTime;
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
            try {
                FileUtils.deleteDirectory(dataDir);
            }
            catch (IOException e) {
                System.err.println(String.format("dataDir %s could not be removed", dataDirectoryName));
            }
        }
    }

}
