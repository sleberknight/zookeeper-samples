package com.nearinfinity.examples.zookeeper.junit4;

import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple JUnit 4 rule (intended to be used as a {@link org.junit.ClassRule}) that starts a Curator
 * {@link TestingServer} before tests run and stops it after tests run.
 */
public class CuratorTestServerRule extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorTestServerRule.class);

    private final int port;
    private TestingServer testingServer;

    public CuratorTestServerRule(int port) {
        this.port = port;
    }

    @Override
    protected void before() throws Throwable {
        testingServer = new TestingServer(port);
        testingServer.start();
    }

    @Override
    protected void after() {
        try {
            testingServer.close();
        } catch (IOException e) {
            LOG.error("Error closing Curator TestingServer on port {}", port, e);
        }
    }
}
