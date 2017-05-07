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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConnectionHelperTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionHelperTest.class);

    private ConnectionHelper connectionHelper;
    private ZooKeeper zooKeeper;

    private static EmbeddedZooKeeperServer embeddedServer;
    private static File dataDirectory;

    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @BeforeClass
    public static void beforeAll() throws IOException, InterruptedException {
        String dataDirectoryName = "target/zookeeper-data";
        dataDirectory = new File(dataDirectoryName);
        deleteDataDirectoryIfExists();

        embeddedServer = new EmbeddedZooKeeperServer(ZK_PORT, dataDirectoryName, 2000);
        embeddedServer.start();
    }

    @AfterClass
    public static void afterAll() throws IOException {
        embeddedServer.shutdown();
        deleteDataDirectoryIfExists();
    }

    private static void deleteDataDirectoryIfExists() throws IOException {
        if (dataDirectory.exists()) {
            FileUtils.deleteDirectory(dataDirectory);
        }
    }

    @Before
    public void setUp() throws InterruptedException {
        connectionHelper = new ConnectionHelper();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (zooKeeper != null) {
            LOG.info("Closing zooKeeper with session id: {}", zooKeeper.getSessionId());
            zooKeeper.close();
            zooKeeper = null;
        }
    }

    @Test
    public void testConnect() throws Exception {
        zooKeeper = connectionHelper.connect(ZK_CONNECTION_STRING);
        assertThat(zooKeeper.getState(), is(ZooKeeper.States.CONNECTED));
        assertThat(zooKeeper.getSessionTimeout(), is(ConnectionHelper.DEFAULT_SESSION_TIMEOUT));
    }

}
