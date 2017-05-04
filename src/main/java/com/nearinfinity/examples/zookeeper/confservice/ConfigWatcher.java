package com.nearinfinity.examples.zookeeper.confservice;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigWatcher implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigWatcher.class);

    private final ActiveKeyValueStore store;

    public ConfigWatcher(String hosts) throws InterruptedException, IOException {
        store = new ActiveKeyValueStore();
        store.connect(hosts);
    }

    public void displayConfig() throws InterruptedException, KeeperException {
        String value = store.read(ConfigUpdater.PATH, this);
        LOG.info("Read {} as {}", ConfigUpdater.PATH, value);
    }


    @Override
    public void process(WatchedEvent event) {
        LOG.info("Process incoming event: {}", event);
        if (event.getType() == Event.EventType.NodeDataChanged) {
            try {
                displayConfig();
            } catch (InterruptedException e) {
                LOG.error("Interrupted. Exiting", e);
                Thread.currentThread().interrupt();
            } catch (KeeperException e) {
                LOG.error("KeeperException: {}", e.code(), e);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigWatcher watcher = new ConfigWatcher(args[0]);
        watcher.displayConfig();

        Thread.sleep(Long.MAX_VALUE);
    }

}
