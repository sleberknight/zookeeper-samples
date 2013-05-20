package com.nearinfinity.examples.zookeeper.confservice;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ConfigWatcher implements Watcher {

    private ActiveKeyValueStore _store;

    public ConfigWatcher(String hosts) throws InterruptedException, IOException {
        _store = new ActiveKeyValueStore();
        _store.connect(hosts);
    }

    public void displayConfig() throws InterruptedException, KeeperException {
        String value = _store.read(ConfigUpdater.PATH, this);
        System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
    }


    @Override
    public void process(WatchedEvent event) {
        System.out.printf("Process incoming event: %s\n", event.toString());
        if (event.getType() == Event.EventType.NodeDataChanged) {
            try {
                displayConfig();
            }
            catch (InterruptedException e) {
                System.err.println("Interrupted. Exiting");
                Thread.currentThread().interrupt();
            }
            catch (KeeperException e) {
                System.err.printf("KeeperException: %s. Exiting.\n", e);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigWatcher watcher = new ConfigWatcher(args[0]);
        watcher.displayConfig();

        Thread.sleep(Long.MAX_VALUE);
    }

}
