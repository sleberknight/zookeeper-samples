package com.nearinfinity.examples.zookeeper.util;

import com.nearinfinity.examples.zookeeper.junit4.EmbeddedZooKeeperServerRule;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConnectionHelperTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionHelperTest.class);

    private static final String DATA_DIRECTORY_NAME = "target/zookeeper-data";
    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @ClassRule
    public static final EmbeddedZooKeeperServerRule ZK_TEST_SERVER =
            new EmbeddedZooKeeperServerRule(ZK_PORT, DATA_DIRECTORY_NAME);

    private ConnectionHelper connectionHelper;
    private ZooKeeper zooKeeper;

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
