package com.nearinfinity.examples.zookeeper.junit.jupiter;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CuratorTestServerExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(CuratorTestServerExtension.class);

    private final int port;
    private TestingServer testingServer;

    public CuratorTestServerExtension(int port) {
        this.port = port;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        testingServer = new TestingServer(port);
        testingServer.start();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        try {
            testingServer.close();
        } catch (IOException e) {
            LOG.error("Error closing Curator TestingServer on port {}", port, e);
        }
    }
}
