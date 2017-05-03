package com.nearinfinity.examples.zookeeper.confservice;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigUpdater.class);

    public static final String PATH = "/config";

    private ActiveKeyValueStore _store;
    private Random _random = new Random();

    public ConfigUpdater(String hosts) throws IOException, InterruptedException {
        _store = new ActiveKeyValueStore();
        _store.connect(hosts);
    }

    @SuppressWarnings("squid:S2189")
    public void run() throws InterruptedException, KeeperException {
        //noinspection InfiniteLoopStatement
        while (true) {
            int value = _random.nextInt(100);
            _store.write(PATH, Integer.toString(value));
            LOG.info("Set {} to {}", PATH, value);
            TimeUnit.SECONDS.sleep(_random.nextInt(10));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigUpdater updater = new ConfigUpdater(args[0]);
        updater.run();
    }

}
