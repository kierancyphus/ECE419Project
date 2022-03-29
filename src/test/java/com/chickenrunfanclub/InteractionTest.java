package com.chickenrunfanclub;

import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.IKVMessage.StatusType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


public class InteractionTest {

    private KVStore kvClient;

    final static int port = 50002;

    @BeforeAll
    static void setup() {
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/Interaction");
        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
    }

    @BeforeEach
    public void setUp() {
        kvClient = new KVStore("./src/test/resources/servers_interaction.cfg");
    }

    @Test
    public void testPut() {
        String key = "foo2";
        String value = "bar2";
        IKVMessage response = null;
        Exception ex = null;
        // delete the key in case exist already
        try {
            kvClient.put(key, null);
        } catch (Exception e) {
        }

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        assertNull(ex);
        assertSame(response.getStatus(), StatusType.PUT_SUCCESS);
    }


    @Test
    public void testUpdate() {
        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        IKVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, initialValue);
            response = kvClient.put(key, updatedValue);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(response.getStatus(), StatusType.PUT_UPDATE);
        assertEquals(response.getValue(), updatedValue);
    }

    @Test
    public void testDelete() {
        String key = "deleteTestValue";
        String value = "toDelete";

        IKVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.put(key, null);

        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(response.getStatus(), StatusType.DELETE_SUCCESS);
    }

    @Test
    public void testGet() {
        String key = "something";
        String value = "else";
        IKVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertEquals(response.getValue(), value);
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an unset value";
        IKVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(response.getStatus(), StatusType.GET_ERROR);
    }
}
