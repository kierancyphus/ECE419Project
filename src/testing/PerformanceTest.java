package testing;

import client.KVStore;
import junit.framework.TestCase;

import java.util.Random;

public class PerformanceTest extends TestCase {
    private static final double NANO_MILLI = 1e6;
    private static final double NANO_SEC = 1e9;
    private static final Random rand = new Random();

    public void test8020() {
        KVStore kvClient = new KVStore("localhost", 50000);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.80, 10000, 200);
//            kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    public void test6535() {
        KVStore kvClient = new KVStore("localhost", 50000);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.65, 10000, 200);
//            kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    public void test5050() {
        KVStore kvClient = new KVStore("localhost", 50000);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.50, 10000, 200);
//            kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    public void test3565() {
        KVStore kvClient = new KVStore("localhost", 50000);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.35, 10000, 200);
//            kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    public void test2080() {
        KVStore kvClient = new KVStore("localhost", 50000);
        Exception ex = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        try {
            runTime(kvClient, 0.20, 10000, 200);
//            kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    public void runTime(KVStore client, double percentPuts, int numRequests, int numUniqueKeys) throws Exception {
        // Currently, caching isn't implemented, so the arguments are useless
        int putCounter = 0;
        int getCounter = 0;
        long reqStart, reqEnd;
        boolean[] putOrGet = new boolean[numRequests];
        long[] reqTime = new long[numRequests];
        try {
            for (int i = 0; i < numRequests; i++) {
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

            long totalAverage = totalTime / numRequests;
            long putAverage = putTime / putCounter;
            long getAverage = getTime / getCounter;
            long throughput = (long) (numRequests / (totalTime / NANO_SEC));
            System.out.println("Put Percentage: " + percentPuts);
            System.out.println("Average Throughput: " + throughput);
            System.out.println("Total time average: " + totalAverage / NANO_MILLI);
            System.out.println("Put time average: " + putAverage / NANO_MILLI);
            System.out.println("\tPut time min: " + putMin / NANO_MILLI);
            System.out.println("\tPut time max: " + putMax / NANO_MILLI);
            System.out.println("Get time average: " + getAverage / NANO_MILLI);
            System.out.println("\tGet time min: " + getMin / NANO_MILLI);
            System.out.println("\tGet time max: " + getMax / NANO_MILLI);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
