package com.nearinfinity.examples.zookeeper.junit4;

import com.nearinfinity.examples.zookeeper.util.EmbeddedZooKeeperServer;
import org.junit.rules.ExternalResource;

import static java.util.Objects.isNull;

/**
 * Simple JUnit 4 rule (intended to be used as a {@link org.junit.ClassRule}) that starts an
 * {@link EmbeddedZooKeeperServer} before tests run and stops it after tests run.
 */
public class EmbeddedZooKeeperServerRule extends ExternalResource {

    private final int port;
    private final String dataDirectoryName;
    private EmbeddedZooKeeperServer embeddedZooKeeper;

    public EmbeddedZooKeeperServerRule(int port) {
        this(port, null);
    }

    public EmbeddedZooKeeperServerRule(int port, String dataDirectoryName) {
        this.port = port;
        this.dataDirectoryName = dataDirectoryName;
    }

    @Override
    protected void before() throws Throwable {
        embeddedZooKeeper = createEmbeddedServer(port, dataDirectoryName);
        embeddedZooKeeper.start();
    }

    private static EmbeddedZooKeeperServer createEmbeddedServer(int port, String dataDirectoryName) {
        if (isNull(dataDirectoryName)) {
            return new EmbeddedZooKeeperServer(port);
        }
        return new EmbeddedZooKeeperServer(port, dataDirectoryName, EmbeddedZooKeeperServer.DEFAULT_TICK_TIME);
    }

    @Override
    protected void after() {
        embeddedZooKeeper.shutdown();
    }
}
