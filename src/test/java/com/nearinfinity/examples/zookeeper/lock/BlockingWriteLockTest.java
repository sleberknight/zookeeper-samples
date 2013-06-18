package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.EmbeddedZooKeeperServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BlockingWriteLockTest {

    private static EmbeddedZooKeeperServer _embeddedServer;
    private ZooKeeper _zooKeeper;
    private String _testLockPath;
    private BlockingWriteLock _writeLock;

    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @BeforeClass
    public static void beforeAll() throws IOException, InterruptedException {
        _embeddedServer = new EmbeddedZooKeeperServer(ZK_PORT);
        _embeddedServer.start();
    }

    @AfterClass
    public static void afterAll() {
        _embeddedServer.shutdown();
    }

    @Before
    public void setUp() throws IOException, InterruptedException {
        _zooKeeper = new ConnectionHelper().connect(ZK_CONNECTION_STRING);
        _testLockPath = "/test-writeLock-" + System.currentTimeMillis();
        _writeLock = new BlockingWriteLock("Test Lock", _zooKeeper, _testLockPath);
    }

    @After
    public void tearDown() throws InterruptedException, KeeperException {
        List<String> children = _zooKeeper.getChildren(_testLockPath, false);
        for (String child : children) {
            _zooKeeper.delete(_testLockPath + "/" + child, -1);
        }
        _zooKeeper.delete(_testLockPath, -1);
    }

    @Test
    public void testLock() throws InterruptedException, KeeperException {
        _writeLock.lock();
        assertNumberOfChildren(_zooKeeper, _testLockPath, 1);
    }

    @Test
    public void testLockWithTimeout() throws InterruptedException, KeeperException {
        boolean obtainedLock = _writeLock.lock(10, TimeUnit.SECONDS);
        assertThat(obtainedLock, is(true));
    }

    @Test
    public void testTryLock() throws InterruptedException, KeeperException {
        boolean obtainedLock = _writeLock.tryLock();
        assertThat(obtainedLock, is(true));
    }

    @Test
    public void testUnlock() throws InterruptedException, KeeperException {
        _writeLock.lock();
        _writeLock.unlock();
        assertNumberOfChildren(_zooKeeper, _testLockPath, 0);
    }

    private void assertNumberOfChildren(ZooKeeper zk, String path, int expectedNumber)
            throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(path, false);
        assertThat(children.size(), is(expectedNumber));
    }
}
