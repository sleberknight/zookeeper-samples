package com.nearinfinity.examples.zookeeper.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.EmbeddedZooKeeperServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class DistributedOperationExecutorTest {

    private static EmbeddedZooKeeperServer _embeddedServer;
    private ZooKeeper _zooKeeper;
    private String _testLockPath;
    private DistributedOperationExecutor _executor;

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
        _testLockPath = "/test-write-lock-" + System.currentTimeMillis();
        _executor = new DistributedOperationExecutor(_zooKeeper);
    }

    @After
    public void tearDown() throws InterruptedException, KeeperException {
        if (_zooKeeper.exists(_testLockPath, false) == null) {
            return;
        }

        List<String> children = _zooKeeper.getChildren(_testLockPath, false);
        for (String child : children) {
            _zooKeeper.delete(_testLockPath + "/" + child, -1);
        }
        _zooKeeper.delete(_testLockPath, -1);
    }

    @Test
    public void testWithLock() throws InterruptedException, KeeperException {
        assertThat(_zooKeeper.exists(_testLockPath, false), is(nullValue()));
        _executor.withLock("Test Lock", _testLockPath, new DistributedOperation<Void>() {
            @Override
            public Void execute() throws DistributedOperationException {
                assertNumberOfChildren(_zooKeeper, _testLockPath, 1);
                return null;
            }
        });
        assertNumberOfChildren(_zooKeeper, _testLockPath, 0);
    }

    @Test
    public void testWithLockHavingSpecifiedTimeout() throws InterruptedException, KeeperException {
        assertThat(_zooKeeper.exists(_testLockPath, false), is(nullValue()));
        final String opResult = "success";
        DistributedOperationResult<String> result = _executor.withLock("Test Lock w/Timeout", _testLockPath,
                new DistributedOperation<String>() {
                    @Override
                    public String execute() throws DistributedOperationException {
                        return opResult;
                    }
                }, 10, TimeUnit.SECONDS);
        assertThat(result.timedOut, is(false));
        assertThat(result.result, is(opResult));
    }

    @Test
    public void testWithLockHavingACLAndHavingSpecifiedTimeout() throws InterruptedException, KeeperException {
        assertThat(_zooKeeper.exists(_testLockPath, false), is(nullValue()));
        final String opResult = "success";
        DistributedOperationResult<String> result = _executor.withLock("Test Lock w/Timeout", _testLockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                new DistributedOperation<String>() {
                    @Override
                    public String execute() throws DistributedOperationException {
                        return opResult;
                    }
                }, 10, TimeUnit.SECONDS);
        assertThat(result.timedOut, is(false));
        assertThat(result.result, is(opResult));
    }

    @Test
    public void testWithLockForMultipleLocksInDifferentThreads() throws InterruptedException, KeeperException {
        assertThat(_zooKeeper.exists(_testLockPath, false), is(nullValue()));
        List<TestDistOp> ops = Arrays.asList(
                new TestDistOp("op-1"),
                new TestDistOp("op-2"),
                new TestDistOp("op-3"),
                new TestDistOp("op-4")
        );

        List<Thread> opThreads = new ArrayList<Thread>();
        for (TestDistOp op : ops) {
            opThreads.add(launchDistributedOperation(op));
            Thread.sleep(10);
        }

        for (Thread opThread : opThreads) {
            opThread.join();
        }

        assertThat(TestDistOp.callCount.get(), is(ops.size()));
        for (TestDistOp op : ops) {
            assertThat(op.executed.get(), is(true));
        }
    }

    private Thread launchDistributedOperation(final TestDistOp op) {
        Thread opThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    _executor.withLock(op.name, _testLockPath, op);
                } catch (Exception ex) {
                    throw new DistributedOperationException(ex);
                }
            }
        });
        opThread.start();
        return opThread;
    }

    static class TestDistOp implements DistributedOperation<Void> {

        static AtomicInteger callCount = new AtomicInteger(0);

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
