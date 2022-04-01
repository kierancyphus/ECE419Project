package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.google.gson.Gson;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class KVServerTest {
    private static final TestUtils utils = new TestUtils();

    @Test
    public void serverReturnsStoppedWithoutInitialization() {
        int port = 50005;
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        ECSNode node = new ECSNode("localhost", port, "0".repeat(32), "F".repeat(32), true, false);
        AllServerMetadata asm = new AllServerMetadata();
        asm.addNode(node);
        server.replaceAllServerMetadata(asm);

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

        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
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

    @Test
    public void dataTransferSuccess() {
        int port = 50007;
        int otherPort = 50008;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/");
        server.clearStorage();
        ECSNode firstNode = new ECSNode("localhost", port, "0".repeat(32), "F".repeat(32), false, false);
        server.updateMetadata(firstNode);
        server.start();

        // initialize server to be transferred to
        KVServer otherServer = new KVServer(otherPort, 10, "FIFO", "./testStore/KVServer/otherServer");
        otherServer.clearStorage();
        // otherServer is only responsible for a subset of hashes
        ECSNode otherServerECSNode = new ECSNode("localhost", otherPort, "0".repeat(32), "9".repeat(32), false, false);
        otherServer.updateMetadata(otherServerECSNode);
        otherServer.start();

        utils.stall(2);

        // populate original server
        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_3.cfg");
        Exception ex = null;
        try {
            for (int i = 0; i < 10; i++) {
                kvClient.put(String.valueOf(i), String.valueOf(i), 0);
            }
        } catch (Exception e) {
            ex = e;
        }

        // update AllServerMetadata for both
        AllServerMetadata allServerMetadata = new AllServerMetadata();
        allServerMetadata.addNode(firstNode);
        allServerMetadata.addNode(otherServerECSNode);
        server.replaceAllServerMetadata(allServerMetadata);
        otherServer.replaceAllServerMetadata(allServerMetadata);

        // transfer data
        server.moveData(allServerMetadata.findServerResponsible("localhost" + 50008, false));
        Set<String> transferredKeys = otherServer.listKeys();
        // these values were computed to be in the range. If the hash function is changed these will change too.
        Set<String> correctKeys = new HashSet<>(Arrays.asList("6", "9"));

        assertNull(ex);
        assertEquals(correctKeys, transferredKeys);
    }

    @Test
    public void serverReturnsWriteLockWhenWriteLockedForPut() {
        int port = 50009;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        server.lockWrite();
        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_4.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.put("key", "value", 0);
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
            kvClient.put("key", "value", 0);

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
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_6.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            kvClient.put("key", null, 0);

            // have to lock and unlock here so we can delete value first
            server.lockWrite();
            server.unLockWrite();
            response = kvClient.put("key", "value", 0);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.PUT_SUCCESS, response.getStatus());
    }

    @Test
    public void serverReturnsNotResponsibleWithMetadataWhenPut() {
        int port = 50012;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        server.clearStorage();
        server.start();


        ECSNode node = new ECSNode("localhost", port, "A".repeat(32), "F".repeat(32), false, false);
        ECSNode otherNode = new ECSNode("localhost", port + 1, "A".repeat(32), "F".repeat(32), false, false);

        AllServerMetadata someMetadata = new AllServerMetadata();
        someMetadata.addNode(node);
        someMetadata.addNode(otherNode);
        server.replaceAllServerMetadata(someMetadata);

        utils.stall(2);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_7.cfg");
        IKVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put("a key it is not responsible for", "who cares", 0);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertSame(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE, response.getStatus());
        assertEquals(response.getKey(), new Gson().toJson(someMetadata, AllServerMetadata.class));
    }

    @Test
    public void serverReturnsNotResponsibleWithMetadataWhenGet() {
        int port = 50013;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/50013");
        server.clearStorage();
        server.start();

        // we have to add a lot of metadata because it only returns not responsible on get when it's not in the chain
        AllServerMetadata someMetadata = new AllServerMetadata();
        List<ECSNode> nodes = utils.generateECSNodes(port);
        nodes.forEach(someMetadata::addNode);

        server.replaceAllServerMetadata(someMetadata);

        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_8.cfg");
        IKVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.get("not in replication chain");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertSame(IKVMessage.StatusType.SERVER_NOT_RESPONSIBLE, response.getStatus());
        assertEquals(new Gson().toJson(someMetadata, AllServerMetadata.class), response.getKey());
    }

    @Test
    public void dataIsReplicatedOnThreeServers() {
        // need to create three running servers and pass them all the right metadata
        List<Integer> ports = Arrays.asList(50014, 50015, 50016);
        List<KVServer> servers = ports
                .stream()
                .map(p -> new KVServer(p, 10, "FIFO", "./testStore/KVServer/" + p))
                .collect(Collectors.toList());
        servers.forEach(KVServer::clearStorage);

        AllServerMetadata asm = new AllServerMetadata();
        ports.forEach(port -> asm.addNode(new ECSNode("localhost", port, null, null, false, false)));
        servers.forEach(s -> s.replaceAllServerMetadata(asm));
        servers.forEach(KVServer::start);

        utils.stall(2);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_9.cfg");
        IKVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put("something", "else", 0);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertNotNull(response);
        assertSame(IKVMessage.StatusType.PUT_SUCCESS, response.getStatus());
        assertEquals(servers.get(0).listKeys(), servers.get(1).listKeys());
        assertEquals(servers.get(0).listKeys(), servers.get(2).listKeys());
        assertEquals(servers.get(2).listKeys(), servers.get(1).listKeys());
    }

    @Test
    public void dataIsReplicatedOnOnlyThreeServers() {
        // need to create three running servers and pass them all the right metadata
        List<Integer> ports = Arrays.asList(50017, 50018, 50019, 50020);
        List<KVServer> servers = ports
                .stream()
                .map(p -> new KVServer(p, 10, "FIFO", "./testStore/KVServer/" + p))
                .collect(Collectors.toList());
        servers.forEach(KVServer::clearStorage);

        AllServerMetadata asm = new AllServerMetadata();
        ports.forEach(port -> asm.addNode(new ECSNode("localhost", port, null, null, false, false)));
        servers.forEach(s -> s.replaceAllServerMetadata(asm));
        servers.forEach(KVServer::start);

        utils.stall(2);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_10.cfg");
        IKVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put("something", "else", 0);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertNotNull(response);
        assertSame(IKVMessage.StatusType.PUT_SUCCESS, response.getStatus());
        assertEquals(servers.get(0).listKeys(), servers.get(3).listKeys());
        assertEquals(servers.get(0).listKeys(), servers.get(1).listKeys());
        assertEquals(servers.get(3).listKeys(), servers.get(1).listKeys());
        assertNotEquals(servers.get(0).listKeys(), servers.get(2).listKeys());
    }

    @Test
    public void deleteGoesThroughChain() {
        // need to create three running servers and pass them all the right metadata
        List<Integer> ports = Arrays.asList(50021, 50022, 50023, 50024);
        List<KVServer> servers = ports
                .stream()
                .map(p -> new KVServer(p, 10, "FIFO", "./testStore/KVServer/" + p))
                .collect(Collectors.toList());
        servers.forEach(KVServer::clearStorage);

        AllServerMetadata asm = new AllServerMetadata();
        ports.forEach(port -> asm.addNode(new ECSNode("localhost", port, null, null, false, false)));
        servers.forEach(s -> s.replaceAllServerMetadata(asm));
        servers.forEach(KVServer::start);

        utils.stall(2);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_11.cfg");
        IKVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put("something", "else", 0);
            response = kvClient.put("something", null, 0);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertNotNull(response);
        assertSame(IKVMessage.StatusType.DELETE_SUCCESS, response.getStatus());
        assertEquals(servers.get(0).listKeys(), servers.get(3).listKeys());
        assertEquals(servers.get(0).listKeys(), servers.get(1).listKeys());
        assertEquals(servers.get(3).listKeys(), servers.get(1).listKeys());
        assertEquals(servers.get(0).listKeys(), servers.get(2).listKeys());
    }
}
