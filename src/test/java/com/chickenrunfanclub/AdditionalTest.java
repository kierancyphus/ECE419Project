package com.chickenrunfanclub;

import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

public class AdditionalTest {
    final static int port = 50006;

    @BeforeAll
    static void setup() {
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/Additional");
        server.clearStorage();
        server.start();
    }
    // TODO add your test cases, at least 3

    @Test
    public void testPersistence() {
        // put and disconnect, then reconnect and get
        String key = "hello";
        String value = "world";
        IKVMessage putResponse = null, getResponse = null;
        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", port);
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
        kvClient = new KVStore("localhost", port);
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
	public void testValueWithSpace() {
		String key = "something";
		String value = "thank u next";
		IKVMessage response = null;
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", port);

		try {
			kvClient.connect();
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

        try {
            kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		if (ex != null) {
			ex.printStackTrace();
		}

		if (response == null) {
			System.out.println("Yikes response is none");
		} else {
			System.out.println("Response: " + response.getValue());
		}

		assertTrue(ex == null && response.getValue().equals(value));
	}

    @Test
    public void testTwoUserPutGet() {
        // two users, one user put the other update
        String key = "hello";
        String value = "world", newValue = "hello!";
        IKVMessage putResponse = null, updateResponse = null, getResponse = null;
        Exception ex = null;

        KVStore kvClient1 = new KVStore("localhost", port);
        KVStore kvClient2 = new KVStore("localhost", port);
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
        // put 1000 pairs, then get them
        IKVMessage putResponse = null, getResponse = null;
        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", port);
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
                kvClient[i] = new KVStore("localhost", port);
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
                kvClient[i] = new KVStore("localhost", port);
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

    @Test
    public void testConcurrencyUpdate() {
        // run n put with t_max threads, then update them
        class Task implements Runnable {
            private KVStore client;
            private int idx;

            public Task(KVStore s, int n) {
                this.client = s;
                this.idx = n;
            }

            public void run() {
                try {
                    client.put(String.valueOf(this.idx), String.valueOf(2 * this.idx));
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
                kvClient[i] = new KVStore("localhost", port);
                kvClient[i].connect();
                kvClient[i].put(String.valueOf(i), String.valueOf(i));
            }
        } catch (Exception e) {
            ex = e;
        }

        // update
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
                assertEquals(getResponse.getValue(), String.valueOf(i * 2));
            }
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null);
    }

    @Test
    public void testEdgeCaseValues() {
        // test some edge cases for messenger
        Exception ex = null;
        KVStore kvClient = new KVStore("localhost", port);

        try {
            kvClient.connect();
            // special characters
            kvClient.put("test1", "~`!@#$%^&*()_+-={}[]|\\:;\"\'<>,.?/");
            // multiple spaces
            kvClient.put("test2", "thank   ,    u      next   !");
            // very long string
            kvClient.put("test3", String.join(" ", Collections.nCopies(10, "hello world")));
            // "null" as key
            kvClient.put("null", "test");
        } catch (Exception e) {
            ex = e;
        }

        try {
            assertEquals(kvClient.get("test1").getValue(), "~`!@#$%^&*()_+-={}[]|\\:;\"\'<>,.?/");
            assertEquals(kvClient.get("test2").getValue(), "thank   ,    u      next   !");
            assertEquals(kvClient.get("test3").getValue(),
                    String.join(" ", Collections.nCopies(10, "hello world")));
            assertEquals(kvClient.get("null").getValue(), "test");
        } catch (Exception e) {
            ex = e;
        }

        // null as value -> it should perform delete
        try {
            assertSame(kvClient.put("test3", null).getStatus(), IKVMessage.StatusType.DELETE_SUCCESS);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    @Test
    public void testDisconnect() {
        // test some edge cases for messenger
        Exception ex = null;
        KVStore kvClient = new KVStore("localhost", port);

        try {
            kvClient.connect();
            kvClient.put("test", "test");
            assertEquals(kvClient.get("test").getValue(), "test");
            kvClient.disconnect();
        } catch (Exception e) {
            ex = e;
        }

        try {
            kvClient.disconnect();
            kvClient.put("test", "new value");
        } catch (Exception e) {    // should throw exception because disconnected
            ex = e;
        }
        assertTrue(ex != null);
        ex = null;

        try {
            kvClient.connect();
            // value should not be changed
            assertEquals(kvClient.get("test").getValue(), "test");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);
    }

}