package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

import com.nearinfinity.examples.zookeeper.junit.jupiter.CuratorTestServerExtension;
import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class BlockingWriteLockTest {

    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @RegisterExtension
    static final CuratorTestServerExtension ZK_TEST_SERVER = new CuratorTestServerExtension(ZK_PORT);

    private ZooKeeper zooKeeper;
    private String testLockPath;
    private BlockingWriteLock writeLock;

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        zooKeeper = new ConnectionHelper().connect(ZK_CONNECTION_STRING);
        testLockPath = "/test-writeLock-" + System.currentTimeMillis();
        writeLock = new BlockingWriteLock("Test Lock", zooKeeper, testLockPath);
    }

    @AfterEach
    void tearDown() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(testLockPath, false);
        for (String child : children) {
            zooKeeper.delete(testLockPath + "/" + child, -1);
        }
        zooKeeper.delete(testLockPath, -1);
    }

    @Test
    void testLock() throws InterruptedException, KeeperException {
        writeLock.lock();
        assertNumberOfChildren(zooKeeper, testLockPath, 1);
    }

    @Test
    void testLockWithTimeout() throws InterruptedException, KeeperException {
        boolean obtainedLock = writeLock.lock(10, TimeUnit.SECONDS);
        assertThat(obtainedLock).isTrue();
    }

    @Test
    void testTryLock() throws InterruptedException, KeeperException {
        boolean obtainedLock = writeLock.tryLock();
        assertThat(obtainedLock).isTrue();
    }

    @Test
    void testUnlock() throws InterruptedException, KeeperException {
        writeLock.lock();
        writeLock.unlock();
        assertNumberOfChildren(zooKeeper, testLockPath, 0);
    }

    private void assertNumberOfChildren(ZooKeeper zk, String path, int expectedNumber)
            throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(path, false);
        assertThat(children).hasSize(expectedNumber);
    }
}
