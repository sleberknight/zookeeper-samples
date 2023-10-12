package com.nearinfinity.examples.zookeeper.util;

import com.nearinfinity.examples.zookeeper.junit.jupiter.CuratorTestServerExtension;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionHelperTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionHelperTest.class);

    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @RegisterExtension
    static final CuratorTestServerExtension ZK_TEST_SERVER = new CuratorTestServerExtension(ZK_PORT);

    private ConnectionHelper connectionHelper;
    private ZooKeeper zooKeeper;

    @BeforeEach
    void setUp() throws InterruptedException {
        connectionHelper = new ConnectionHelper();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (zooKeeper != null) {
            LOG.info("Closing zooKeeper with session id: {}", zooKeeper.getSessionId());
            zooKeeper.close();
            zooKeeper = null;
        }
    }

    @Test
    void testConnect() throws Exception {
        zooKeeper = connectionHelper.connect(ZK_CONNECTION_STRING);
        assertThat(zooKeeper.getState()).isEqualTo(ZooKeeper.States.CONNECTED);
        assertThat(zooKeeper.getSessionTimeout()).isEqualTo(ConnectionHelper.DEFAULT_SESSION_TIMEOUT);
    }

}
