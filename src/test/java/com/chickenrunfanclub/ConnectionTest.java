package com.chickenrunfanclub;

import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ConnectionTest {
    final static int port = 50001;

    @BeforeAll
    static void setup() {
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/Connection");
        server.clearStorage();
        server.start();
    }
    
    
    @Test
    public void testConnectionSuccess() {

        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", port);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    @Test
    public void testUnknownHost() {
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", port);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
    }

    @Test
    public void testIllegalPort() {
        Exception ex = null;
        KVStore kvClient = new KVStore("localhost", 123456789);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }
}

