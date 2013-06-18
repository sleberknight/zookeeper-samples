package com.nearinfinity.examples.zookeeper.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConnectionHelperTest {

    private ConnectionHelper _connectionHelper;
    private ZooKeeper _zooKeeper;

    private static EmbeddedZooKeeperServer _embeddedServer;
    private static File _dataDirectory;

    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @BeforeClass
    public static void beforeAll() throws IOException, InterruptedException {
        String dataDirectoryName = "target/zookeeper-data";
        _dataDirectory = new File(dataDirectoryName);
        deleteDataDirectoryIfExists();

        _embeddedServer = new EmbeddedZooKeeperServer(ZK_PORT, dataDirectoryName, 2000);
        _embeddedServer.start();
    }

    @AfterClass
    public static void afterAll() throws IOException {
        _embeddedServer.shutdown();
        deleteDataDirectoryIfExists();
    }

    private static void deleteDataDirectoryIfExists() throws IOException {
        if (_dataDirectory.exists()) {
            FileUtils.deleteDirectory(_dataDirectory);
        }
    }

    @Before
    public void setUp() throws InterruptedException {
        _connectionHelper = new ConnectionHelper();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (_zooKeeper != null) {
            System.out.printf("Closing zooKeeper with session id: %d\n", _zooKeeper.getSessionId());
            _zooKeeper.close();
            _zooKeeper = null;
        }
    }

    @Test
    public void testConnect() throws Exception {
        _zooKeeper = _connectionHelper.connect(ZK_CONNECTION_STRING);
        assertThat(_zooKeeper.getState(), is(ZooKeeper.States.CONNECTED));
        assertThat(_zooKeeper.getSessionTimeout(), is(ConnectionHelper.DEFAULT_SESSION_TIMEOUT));
    }

}
