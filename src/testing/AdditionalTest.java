package testing;

import client.KVStore;
import org.junit.Test;

import junit.framework.TestCase;
import shared.messages.IKVMessage;

import java.util.concurrent.*;

public class AdditionalTest extends TestCase {

    // TODO add your test cases, at least 3

    @Test
    public void testPersistence() {
        // put and disconnect, then reconnect and get
        String key = "hello";
        String value = "world";
        IKVMessage putResponse = null, getResponse = null;
        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        // delete the key value pair in case already exist
        try {
            kvClient.put(key, null);
        } catch (Exception e) {
        }
        // put
        try {
            putResponse = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        // disconnect
        kvClient.disconnect();
        kvClient = null;
        // get
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
            getResponse = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && putResponse.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && getResponse.getValue().equals(value));
    }


    @Test
    public void testTwoUserPutGet() {
        // two users, one user put the other update
        String key = "hello";
        String value = "world", newValue = "hello!";
        IKVMessage putResponse = null, updateResponse = null, getResponse = null;
        Exception ex = null;

        KVStore kvClient1 = new KVStore("localhost", 50000);
        KVStore kvClient2 = new KVStore("localhost", 50000);
        try {
            kvClient1.connect();
            kvClient2.connect();
        } catch (Exception e) {
            ex = e;
        }
        // delete the key value pair in case already exist
        try {
            kvClient1.put(key, null);
        } catch (Exception e) {
        }

        // user 1 put
        try {
            putResponse = kvClient1.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        // user 2 updates it
        try {
            updateResponse = kvClient2.put(key, newValue);
        } catch (Exception e) {
            ex = e;
        }

        // user 1 get
        try {
            getResponse = kvClient1.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && putResponse.getStatus() == IKVMessage.StatusType.PUT_SUCCESS
                && updateResponse.getStatus() == IKVMessage.StatusType.PUT_UPDATE
                && getResponse.getValue().equals(newValue));
    }

    @Test
    public void testCacheCapacity() {
        // put  1000 pairs, then get them
        IKVMessage putResponse = null, getResponse = null;
        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        // put 10000 pairs
        try {
            for (int i = 0; i < 1000; i++) {
                kvClient.put(String.valueOf(i), String.valueOf(i));
            }
        } catch (Exception e) {
            ex = e;
        }

        // get
        try {
            for (int i = 0; i < 1000; i++) {
                getResponse = kvClient.get(String.valueOf(i));
                assertTrue(ex == null && getResponse.getValue().equals(String.valueOf(i)));
            }
        } catch (Exception e) {
            ex = e;
        }
    }

    @Test
    public void testConcurrencyPut() {
        // run n put with t_max threads
        class Task implements Runnable {
            private final KVStore client;
            private final int idx;

            public Task(KVStore s, int n) {
                client = s;
                idx = n;
            }

            public void run() {
                try {
                    client.put(String.valueOf(idx), String.valueOf(idx));
                } catch (Exception e) {
                }
            }
        }

        IKVMessage putResponse = null, getResponse = null;
        Exception ex = null;
        int n = 200, t_max = 100;
        KVStore[] kvClient = new KVStore[n];
        Runnable[] tasks = new Task[n];

        try {
            for (int i = 0; i < n; i++) {
                kvClient[i] = new KVStore("localhost", 50000);
                kvClient[i].connect();
            }
        } catch (Exception e) {
            ex = e;
        }

        ExecutorService pool = Executors.newFixedThreadPool(t_max);

        try {
            for (int i = 0; i < n; i++) {
                tasks[i] = new Task(kvClient[i], i);
                pool.execute(tasks[i]);
            }
        } catch (Exception e) {
            ex = e;
        }
        pool.shutdown();

        // get them
        try {
            for (int i = 0; i < n; i++) {
                getResponse = kvClient[0].get(String.valueOf(i));
                assertEquals(getResponse.getValue(), String.valueOf(i));
            }
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null);
    }

    @Test
    public void testConcurrencyGet() {
        // run n get with t_max threads
        class Task implements Callable<String> {
            private final KVStore client;
            private final int idx;

            public Task(KVStore s, int n) {
                client = s;
                idx = n;
            }

            @Override
            public String call() throws Exception {
                try {
                    return client.get(String.valueOf(idx)).getValue();
                } catch (Exception e) {
                }
                return "";
            }
        }

        IKVMessage putResponse = null, getResponse = null;
        Exception ex = null;
        int n = 200, t_max = 100;
        KVStore[] kvClient = new KVStore[n];
        Task[] tasks = new Task[n];

        try {
            for (int i = 0; i < n; i++) {
                kvClient[i] = new KVStore("localhost", 50000);
                kvClient[i].connect();
            }
        } catch (Exception e) {
            ex = e;
        }

        try {
            for (int i = 0; i < n; i++) {
                kvClient[0].put(String.valueOf(i), String.valueOf(i));
                ;
            }
        } catch (Exception e) {
            ex = e;
        }

        ExecutorService executor = Executors.newFixedThreadPool(t_max);

        // get them
        try {
            for (int i = 0; i < n; i++) {
                tasks[i] = new Task(kvClient[i], i);
                Future<String> result = executor.submit(tasks[i]);
                assertEquals(result.get(), String.valueOf(i));
            }
        } catch (Exception e) {
            ex = e;
        }

        executor.shutdown();
        assertTrue(ex == null);
    }

}
