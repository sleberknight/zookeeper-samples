package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.nearinfinity.examples.zookeeper.junit4.CuratorTestServerRule;
import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class DistributedOperationExecutorTest {

    private static final int ZK_PORT = 53181;
    private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

    @ClassRule
    public static final CuratorTestServerRule ZK_TEST_SERVER = new CuratorTestServerRule(ZK_PORT);

    private ZooKeeper zooKeeper;
    private String testLockPath;
    private DistributedOperationExecutor executor;

    @Before
    public void setUp() throws IOException, InterruptedException {
        zooKeeper = new ConnectionHelper().connect(ZK_CONNECTION_STRING);
        testLockPath = "/test-write-lock-" + System.currentTimeMillis();
        executor = new DistributedOperationExecutor(zooKeeper);
    }

    @After
    public void tearDown() throws InterruptedException, KeeperException {
        if (zooKeeper.exists(testLockPath, false) == null) {
            return;
        }

        List<String> children = zooKeeper.getChildren(testLockPath, false);
        for (String child : children) {
            zooKeeper.delete(testLockPath + "/" + child, -1);
        }
        zooKeeper.delete(testLockPath, -1);
    }

    @Test
    public void testWithLock() throws InterruptedException, KeeperException {
        assertThat(zooKeeper.exists(testLockPath, false), is(nullValue()));
        executor.withLock("Test Lock", testLockPath, () -> {
            assertNumberOfChildren(zooKeeper, testLockPath, 1);
            return null;
        });
        assertNumberOfChildren(zooKeeper, testLockPath, 0);
    }

    @Test
    public void testWithLockHavingSpecifiedTimeout() throws InterruptedException, KeeperException {
        assertThat(zooKeeper.exists(testLockPath, false), is(nullValue()));
        final String opResult = "success";
        DistributedOperationResult<String> result = executor.withLock("Test Lock w/Timeout", testLockPath,
                () -> opResult, 10, TimeUnit.SECONDS);
        assertThat(result.timedOut, is(false));
        assertThat(result.result, is(opResult));
    }

    @Test
    public void testWithLockHavingACLAndHavingSpecifiedTimeout() throws InterruptedException, KeeperException {
        assertThat(zooKeeper.exists(testLockPath, false), is(nullValue()));
        final String opResult = "success";
        DistributedOperationResult<String> result = executor.withLock("Test Lock w/Timeout", testLockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                () -> opResult, 10, TimeUnit.SECONDS);
        assertThat(result.timedOut, is(false));
        assertThat(result.result, is(opResult));
    }

    @Test
    public void testWithLockForMultipleLocksInDifferentThreads() throws InterruptedException, KeeperException {
        assertThat(zooKeeper.exists(testLockPath, false), is(nullValue()));
        List<TestDistOp> ops = Arrays.asList(
                new TestDistOp("op-1"),
                new TestDistOp("op-2"),
                new TestDistOp("op-3"),
                new TestDistOp("op-4")
        );

        List<Thread> opThreads = new ArrayList<>();
        for (TestDistOp op : ops) {
            opThreads.add(launchDistributedOperation(op));
            Thread.sleep(10);
        }

        long maxWaitTimeMillis = TimeUnit.SECONDS.toMillis(5);
        for (Thread opThread : opThreads) {
            opThread.join(maxWaitTimeMillis);
        }

        assertThat(TestDistOp.callCount.get(), is(ops.size()));
        for (TestDistOp op : ops) {
            assertThat(op.executed.get(), is(true));
        }
    }

    private Thread launchDistributedOperation(final TestDistOp op) {
        Thread opThread = new Thread(() -> {
            try {
                executor.withLock(op.name, testLockPath, op);
            } catch (Exception ex) {
                throw new DistributedOperationException(ex);
            }
        });
        opThread.start();
        return opThread;
    }

    static class TestDistOp implements DistributedOperation<Void> {

        static final AtomicInteger callCount = new AtomicInteger(0);

        final String name;
        final AtomicBoolean executed;

        TestDistOp(String name) {
            this.name = name;
            this.executed = new AtomicBoolean(false);
        }

        @Override
        public Void execute() throws DistributedOperationException {
            callCount.incrementAndGet();
            executed.set(true);
            return null;
        }
    }

    private void assertNumberOfChildren(ZooKeeper zk, String path, int expectedNumber) {
        List<String> children;
        try {
            children = zk.getChildren(path, false);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        assertThat(children.size(), is(expectedNumber));
    }
}
