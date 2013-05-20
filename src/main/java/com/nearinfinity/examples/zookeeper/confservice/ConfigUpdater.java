package com.nearinfinity.examples.zookeeper.confservice;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

public class ConfigUpdater {

    public static final String PATH = "/config";

    private ActiveKeyValueStore _store;
    private Random _random = new Random();

    public ConfigUpdater(String hosts) throws IOException, InterruptedException {
        _store = new ActiveKeyValueStore();
        _store.connect(hosts);
    }

    public void run() throws InterruptedException, KeeperException {
        //noinspection InfiniteLoopStatement
        while (true) {
            String value = _random.nextInt(100) + "";
            _store.write(PATH, value);
            System.out.printf("Set %s to %s\n", PATH, value);
            TimeUnit.SECONDS.sleep(_random.nextInt(10));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigUpdater updater = new ConfigUpdater(args[0]);
        updater.run();
    }

}
