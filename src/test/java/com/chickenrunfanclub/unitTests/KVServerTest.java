package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.google.gson.Gson;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class KVServerTest {
    private static final TestUtils utils = new TestUtils();

    @Test
    public void serverReturnsStoppedWithoutInitialization() {
        int port = 50005;
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer");
        server.clearStorage();
        server.start();
        utils.stall(2);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_1.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.get("something");
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.SERVER_STOPPED, response.getStatus());
    }

    @Test
    public void serverNotReturnsStoppedAfterInitialization() {
        int port = 50006;

        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer");
        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_2.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.get("something");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertNotSame(response.getStatus(), IKVMessage.StatusType.SERVER_STOPPED);
    }

    @Disabled
    @Test
    public void dataTransferSuccess() {
        int port = 50007;
        int otherPort = 50008;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer");
        server.clearStorage();
        server.updateMetadata(new ECSNode("localhost", port, "0".repeat(32), "F".repeat(32), false, false));
        server.start();

        // initialize server to be transferred to
        KVServer otherServer = new KVServer(otherPort, 10, "FIFO", "./testStore/KVServer/otherServer");
        otherServer.clearStorage();
        // otherServer is only responsible for a subset of hashes
        ECSNode otherServerECSNode = new ECSNode("localhost", otherPort, "0".repeat(32), "9".repeat(32), false, false);
        otherServer.updateMetadata(otherServerECSNode);
        otherServer.start();
        utils.stall(5);

        // populate original server
        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_3.cfg");
        Exception ex = null;
        try {
            for (int i = 0; i < 10; i++) {
                kvClient.put(String.valueOf(i), String.valueOf(i));
            }
        } catch (Exception e) {
            ex = e;
        }

        // transfer data
        server.moveData(otherServerECSNode);
        Set<String> transferredKeys = otherServer.listKeys();
        // these values were computed to be in the range. If the hash function is changed these will change too.
        Set<String> correctKeys = new HashSet<>(Arrays.asList("6", "7", "9"));

        assertNull(ex);
        assertEquals(correctKeys, transferredKeys);
    }

    @Test
    public void serverReturnsWriteLockWhenWriteLockedForPut() {
        int port = 50009;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer");
        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        server.lockWrite();
        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_4.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.put("key", "value");
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.SERVER_WRITE_LOCK, response.getStatus());
    }

    @Test
    public void serverReturnsGetWhenWriteLockedForGet() {
        int port = 50010;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer");
        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_5.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            kvClient.put("key", "value");

            // have to lock here so we can make sure the key is in the server
            server.lockWrite();
            response = kvClient.get("key");
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.GET_SUCCESS, response.getStatus());
    }

    @Test
    public void serverReturnsPutWhenWriteLockUnlockedForPut() {
        int port = 50011;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer");
        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_6.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            kvClient.put("key", null);

            // have to lock and unlock here so we can delete value first
            server.lockWrite();
            server.unLockWrite();
            response = kvClient.put("key", "value");
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.PUT_SUCCESS, response.getStatus());
    }

    // TODO: When cluster metadata has been created, the below needs to be updated to return the new metadata.

    @Disabled
    @Test
    public void serverReturnsNotResponsibleWithMetadataWhenPut() {
        int port = 50012;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer");
        server.clearStorage();
        server.start();
        server.updateMetadata(new ECSNode("localhost", port, "A".repeat(32), "F".repeat(32), false, false));
        AllServerMetadata someMetadata = new AllServerMetadata(new HashMap<>());
        server.replaceAllServerMetadata(someMetadata);
        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_7.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.put("6", "value");
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE, response.getStatus());
        assertEquals(response.getKey(), new Gson().toJson(someMetadata, AllServerMetadata.class));
    }

    @Disabled
    @Test
    public void serverReturnsNotResponsibleWithMetadataWhenGet() {
        int port = 50013;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer");
        server.clearStorage();
        server.start();
        server.updateMetadata(new ECSNode("localhost", port, "A".repeat(32), "F".repeat(32), false, false));
        AllServerMetadata someMetadata = new AllServerMetadata(new HashMap<>());
        server.replaceAllServerMetadata(someMetadata);
        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_8.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.get("6");
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE, response.getStatus());
        assertEquals(response.getKey(), new Gson().toJson(someMetadata, AllServerMetadata.class));
    }
}
