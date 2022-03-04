package com.chickenrunfanclub;

import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertNull;

public class PerformanceTestFIFO {
    private static final double NANO_MILLI = 1e6;
    private static final double NANO_SEC = 1e9;
    private static final Random rand = new Random();
    final static TestUtils utils = new TestUtils();
    final static int port = 50021;

    @BeforeAll
    static void setup() {
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/Performance");
        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        utils.stall(1);
    }
    
    @Disabled
    @Test
    public void test8020() {
        KVStore kvClient = new KVStore("localhost", port);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.80, 10000, 200);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    @Disabled
    @Test
    public void test6535() {
        KVStore kvClient = new KVStore("localhost", port);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.65, 10000, 200);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    @Disabled
    @Test
    public void test5050() {
        KVStore kvClient = new KVStore("localhost", port);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.50, 10000, 200);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    @Disabled
    @Test
    public void test3565() {
        KVStore kvClient = new KVStore("localhost", port);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.35, 10000, 200);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    @Disabled
    @Test
    public void test2080() {
        KVStore kvClient = new KVStore("localhost", port);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.20, 10000, 200);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    public void runTime(KVStore client, double percentPuts, int numRequests, int numUniqueKeys) throws Exception {
        // Currently, caching isn't implemented
        int putCounter = 0;
        int getCounter = 0;
        long reqStart, reqEnd;
        boolean[] putOrGet = new boolean[numRequests];
        long[] reqTime = new long[numRequests];
        try {
            for (int i = 0; i < numRequests; i++) {
                // Randomly choose to do put or gets based on the percentage split
                boolean doPut = rand.nextDouble() < percentPuts;
                String rand_key = "key" + rand.nextInt(numUniqueKeys);
                String rand_value = "key" + rand.nextInt(numRequests);
                reqStart = System.nanoTime();
                if (doPut) {
                    try {
                        client.put(rand_key, rand_value);
                    } catch (Exception e) {
                        throw e;
                    }
                    putOrGet[i] = true;
                } else {
                    try {
                        client.get(rand_key);
                    } catch (Exception e) {
                        throw e;
                    }
                    putOrGet[i] = false;
                }
                reqEnd = System.nanoTime();
                reqTime[i] = (reqEnd - reqStart);
            }
            // Calculate the total time for puts and gets
            long putTime, getTime, putMax, putMin, getMax, getMin;
            putTime = getTime = 0;
            putMax = putMin = getMax = getMin = reqTime[0];
            for (int i = 0; i < numRequests; i++) {
                if (putOrGet[i]) {
                    putCounter++;
                    putMin = Math.min(putMin, reqTime[i]);
                    putMax = Math.max(putMax, reqTime[i]);
                    putTime += reqTime[i];
                } else {
                    getCounter++;
                    getMin = Math.min(getMin, reqTime[i]);
                    getMax = Math.max(getMax, reqTime[i]);
                    getTime += reqTime[i];
                }
            }
            long totalTime = getTime + putTime;
            // Calcualte the per request latency, and print values out.
            long totalAverage = totalTime / numRequests;
            long putAverage = putTime / putCounter;
            long getAverage = getTime / getCounter;
            long throughput = (long) (numRequests / (totalTime / NANO_SEC));
            try {
                File file = new File("perf_FIFO.csv");
                FileWriter fr = new FileWriter(file, true);
                fr.write(percentPuts+ "," +  throughput + "," + totalAverage / NANO_MILLI + "," +
                        putAverage / NANO_MILLI + "," + putMin / NANO_MILLI + "," + putMax / NANO_MILLI + "," +
                        getAverage / NANO_MILLI + "," + getMin / NANO_MILLI + "," + getMax / NANO_MILLI + "," + "\n");
                fr.close();
            } catch (FileNotFoundException e) {
                System.out.println(e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
