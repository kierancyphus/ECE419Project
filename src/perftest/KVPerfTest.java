package perftest;

import app_kvServer.KVServer;
import java.util.Random;

public class KVPerfTest {
    private static final double NANO_MILLI = 1e6;
    private static final Random rand = new Random();
    /**
     * Main entry point for the KV server performance evaluation.
     * @param args should contain port number, cache size, cache strategy (FIFO, LRU, LFU).
     */
    public static void main(String[] args){
        try{
            if (args.length != 3){
                System.out.println("Error: Invalid number of args!");
                System.out.println("Argument order: <percentage puts> " +
                                   "<# requests> <# unique keys>");

                double percentPuts = Integer.parseInt(args[0]) / 100.0;
                int numRequests = Integer.parseInt(args[1]);
                int numUniqueKeys = Integer.parseInt(args[2]);
                // Currently, caching isn't implemented, so the arguments are useless
                KVServer server = new KVServer(33333, 1,"FIFO");
                int putCounter = 0;
                int getCounter = 0;
                long reqStart, reqEnd;
                boolean[] putOrGet = new boolean[numRequests];
                long[] reqTime = new long[numRequests];
                
                for(int i = 0; i < numRequests; i++){
                    boolean doPut = rand.nextDouble() < percentPuts;
                    String rand_key = "key" + rand.nextInt();
                    reqStart = System.nanoTime();
                    if(doPut) {
                        try {
                            server.putKV(rand_key, "test_value");
                        } catch (Exception e) {}
                        putOrGet[i] = true;
                    } else {
                        try {
                            server.getKV(rand_key);
                        } catch (Exception e) {}
                        putOrGet[i] = false;
                    }
                    reqEnd = System.nanoTime();
                    reqTime[i] = (reqEnd - reqStart);
                }
                
                long putTime, getTime, putMax, putMin, getMax, getMin;
                putTime = getTime = 0;
                putMax = putMin = getMax = getMin = reqTime[0];
                for(int i = 0; i < numRequests; i++){
                    if(putOrGet[i]) {
                        putCounter++;
                        putMin = Math.min(putMin, reqTime[i]);
                        putMax = Math.max(putMax, reqTime[i]);
                        putTime += reqTime[i];
                    }else {
                        getCounter++;
                        getMin = Math.min(getMin, reqTime[i]);
                        getMax = Math.max(getMax, reqTime[i]);
                        getTime += reqTime[i];
                    }
                }
                long totalTime = getTime + putTime;

                long totalAverage = totalTime / numRequests;
                long getAverage = getTime / getCounter;
                long putAverage = putTime / putCounter;
                System.out.println("Total time average: " + totalAverage/NANO_MILLI);
                System.out.println("Get time average: " + getAverage/NANO_MILLI);
                System.out.println("\tGet time min: " + getMin/NANO_MILLI);
                System.out.println("\tGet time max: " + getMax/NANO_MILLI);
                System.out.println("Put time average: " + putAverage/NANO_MILLI);
                System.out.println("\tPut time min: " + putMin/NANO_MILLI);
                System.out.println("\tPut time max: " + putMax/NANO_MILLI);

            }

        }catch (NumberFormatException e){
            System.out.println("Error: Invalid args!");
            System.out.println("Argument order:  <percentage puts> " +
                    "<# requests> <# unique keys>");
            System.exit(1);
        }
    }
}
