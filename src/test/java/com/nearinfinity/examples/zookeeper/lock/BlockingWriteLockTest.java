package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.examples.zookeeper.junit4.CuratorTestServerRule;
import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BlockingWriteLockTest {

    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @ClassRule
    public static final CuratorTestServerRule ZK_TEST_SERVER = new CuratorTestServerRule(ZK_PORT);

//    @ClassRule
//    public static final EmbeddedZooKeeperServerRule ZK_TEST_SERVER = new EmbeddedZooKeeperServerRule(ZK_PORT);

    private ZooKeeper zooKeeper;
    private String testLockPath;
    private BlockingWriteLock writeLock;

    @Before
    public void setUp() throws IOException, InterruptedException {
        zooKeeper = new ConnectionHelper().connect(ZK_CONNECTION_STRING);
        testLockPath = "/test-writeLock-" + System.currentTimeMillis();
        writeLock = new BlockingWriteLock("Test Lock", zooKeeper, testLockPath);
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
    public void testLock() throws InterruptedException, KeeperException {
        writeLock.lock();
        assertNumberOfChildren(zooKeeper, testLockPath, 1);
    }

    @Test
    public void testLockWithTimeout() throws InterruptedException, KeeperException {
        boolean obtainedLock = writeLock.lock(10, TimeUnit.SECONDS);
        assertThat(obtainedLock, is(true));
    }

    @Test
    public void testTryLock() throws InterruptedException, KeeperException {
        boolean obtainedLock = writeLock.tryLock();
        assertThat(obtainedLock, is(true));
    }

    @Test
    public void testUnlock() throws InterruptedException, KeeperException {
        writeLock.lock();
        writeLock.unlock();
        assertNumberOfChildren(zooKeeper, testLockPath, 0);
    }

    private void assertNumberOfChildren(ZooKeeper zk, String path, int expectedNumber)
            throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(path, false);
        assertThat(children.size(), is(expectedNumber));
    }
}
