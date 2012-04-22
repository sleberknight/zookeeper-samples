package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.EmbeddedZooKeeperServer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class DistributedOperationExecutorTest {

    private static EmbeddedZooKeeperServer embeddedServer;
    private ZooKeeper zooKeeper;
    private String testLockPath;
    private DistributedOperationExecutor executor;

    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @BeforeClass
    public static void beforeAll() throws IOException, InterruptedException {
        embeddedServer = new EmbeddedZooKeeperServer(ZK_PORT);
        embeddedServer.start();
    }

    @AfterClass
    public static void afterAll() {
        embeddedServer.shutdown();
    }

    @Before
    public void setUp() throws IOException, InterruptedException {
        zooKeeper = new ConnectionHelper().connect(ZK_CONNECTION_STRING);
        testLockPath = "/test-writeLock-" + System.currentTimeMillis();
        executor = new DistributedOperationExecutor(zooKeeper);
    }

    @After
    public void tearDown() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(testLockPath, false);
        for (String child : children) {
            zooKeeper.delete(testLockPath + "/" + child, -1);
        }
        zooKeeper.delete(testLockPath, -1);
    }

    @Test
    public void testWithLock() throws InterruptedException, KeeperException {
        assertThat(zooKeeper.exists(testLockPath, false), is(nullValue()));
        executor.withLock("Test Lock", testLockPath, new DistributedOperation() {
            @Override
            public Object execute() throws DistributedOperationException {
                assertNumberOfChildren(zooKeeper, testLockPath, 1);
                return null;
            }
        });
        assertNumberOfChildren(zooKeeper, testLockPath, 0);
    }

    private void assertNumberOfChildren(ZooKeeper zk, String path, int expectedNumber) {
        List<String> children;
        try {
            children = zk.getChildren(path, false);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        assertThat(children.size(), is(expectedNumber));
    }
}
