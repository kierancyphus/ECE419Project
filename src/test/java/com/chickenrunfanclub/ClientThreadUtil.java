package com.chickenrunfanclub;

import com.chickenrunfanclub.client.KVStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ClientThreadUtil implements Runnable {
    private static final double NANO_MILLI = 1e6;
    private static final double NANO_SEC = 1e9;
    private final List<ArrayList<String>> data;
    private final KVStore client;
    private final boolean populate;
    private final double putRatio;
    private final Random rand;
    double putTime, getTime = 0;
    int getCounter, putCounter = 0;

    public ClientThreadUtil(String config, List<ArrayList<String>> data, boolean populate, double putRatio) {
        this.client = new KVStore(config);
        this.data = data;
        this.populate = populate;
        this.putRatio = putRatio;
        this.rand = new Random();
    }

    @Override
    public void run() {
        // Currently, caching isn't implemented
        int numRequests = this.data.size();
        double reqStart, reqEnd;
        boolean[] putOrGet = new boolean[this.data.size()];
        double[] reqTime = new double[this.data.size()];
        try {
            for (int i = 0; i < numRequests; i++) {


                ArrayList<String> pair = data.get(i);
                String key = pair.get(0);
                String value = pair.get(1);

                if (populate) {
                    try {
                        client.put(key, value);
                        continue;
                    } catch (Exception e) {
                        throw e;
                    }
                } else {
                    reqStart = System.nanoTime();
                    if (rand.nextDouble() < putRatio) {
                        try {
                            client.put(key, value);
                        } catch (Exception e) {
                            throw e;
                        }
                        putOrGet[i] = true;
                    } else {
                        try {
                            client.get(key);
                        } catch (Exception e) {
                            throw e;
                        }
                        putOrGet[i] = false;
                    }
                    reqEnd = System.nanoTime();
                    reqTime[i] = (reqEnd - reqStart);
                }
            }
//             Calculate the total time for puts and gets

            for (int i = 0; i < numRequests; i++) {
                if (putOrGet[i]) {
                    putCounter++;
                    putTime += reqTime[i];
                } else {
                    getCounter++;
                    getTime += reqTime[i];
                }
            }
            getTime = getTime / NANO_MILLI;
            putTime = putTime / NANO_MILLI;

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}