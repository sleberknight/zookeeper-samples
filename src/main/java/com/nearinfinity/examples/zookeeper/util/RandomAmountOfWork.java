package com.nearinfinity.examples.zookeeper.util;

import java.util.Random;

public final class RandomAmountOfWork {

    private Random random = new Random(System.currentTimeMillis());

    public int timeItWillTake() {
        return 5 + random.nextInt(5);  // sample work takes 5-10 seconds
    }

}
