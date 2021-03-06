package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVInternalStore;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.IServerMessage;
import com.google.gson.Gson;
import org.junit.jupiter.api.Disabled;
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
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);

        server.clearStorage();
        server.start();
        utils.stall(2);

        KVInternalStore client = new KVInternalStore(asm);

        IKVMessage response = null;
        Exception ex = null;
        try {
            response = client.get("something");
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

        ECSNode node = new ECSNode("localhost", port, "0".repeat(32), "F".repeat(32), true, false);
        AllServerMetadata asm = new AllServerMetadata();
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);

        server.clearStorage();
        server.start();
        utils.stall(1);

        KVInternalStore client = new KVInternalStore(asm);
        IKVMessage response = null;
        Exception ex = null;


        try {
            client.start("localhost", port);
            response = client.get("something");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
        assertNotSame(response.getStatus(), IKVMessage.StatusType.SERVER_STOPPED);
    }

    @Disabled
    @Test
    public void serverUpdatesAllMetadata() {
        int port = 50006;

        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        AllServerMetadata asm = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", port, null, null, false, false);
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);

        server.clearStorage();
        server.start();
        utils.stall(1);

        KVInternalStore client = new KVInternalStore(asm);
        Exception ex = null;

        asm = new AllServerMetadata();


        try {
            client.start("localhost", port);
            client.updateAllMetadata(asm, "localhost", port);
//            response = kvClient.get("something");
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
//        assertNotSame(response.getStatus(), IKVMessage.StatusType.SERVER_STOPPED);
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
        AllServerMetadata clientMetadata = new AllServerMetadata();
        clientMetadata.addNodeToHashRing(firstNode);
        KVInternalStore client = new KVInternalStore(clientMetadata);

        Exception ex = null;
        try {
            for (int i = 0; i < 10; i++) {
                client.put(String.valueOf(i), String.valueOf(i), 0);
            }
        } catch (Exception e) {
            ex = e;
        }

        // update AllServerMetadata for both
        AllServerMetadata allServerMetadata = new AllServerMetadata();
        allServerMetadata.addNodeToHashRing(firstNode);
        allServerMetadata.addNodeToHashRing(otherServerECSNode);
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
        AllServerMetadata asm = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", port, null, null, false, false);
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);

        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        server.lockWrite();
        utils.stall(1);

        KVInternalStore client = new KVInternalStore(asm);
        IKVMessage response = null;
        Exception ex = null;
        try {
            response = client.put("key", "value", 0);
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
        AllServerMetadata asm = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", port, null, null, false, false);
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);

        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        utils.stall(1);

        KVInternalStore client = new KVInternalStore(asm);
        IKVMessage response = null;
        Exception ex = null;
        try {
            client.put("key", "value", 0);

            // have to lock here so we can make sure the key is in the server
            server.lockWrite();
            response = client.get("key");
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
        AllServerMetadata asm = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", port, null, null, false, false);
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);

        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        utils.stall(1);

        KVInternalStore client = new KVInternalStore(asm);
        IKVMessage response = null;
        Exception ex = null;
        try {
            // have to lock and unlock here so we can delete value first
            server.lockWrite();
            server.unLockWrite();
            response = client.put("key", "value", 0);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.PUT_SUCCESS, response.getStatus());
    }

    @Disabled
    @Test
    public void serverReturnsNotResponsibleWithMetadataWhenPut() {
        // Note for M4, we no longer need to send an update of the metadata since everything that needs it will be
        // directly updated by ECS

        int port = 50012;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        server.clearStorage();
        server.start();


        ECSNode node = new ECSNode("localhost", port, "A".repeat(32), "F".repeat(32), false, false);
        ECSNode otherNode = new ECSNode("localhost", port + 1, "A".repeat(32), "F".repeat(32), false, false);

        AllServerMetadata someMetadata = new AllServerMetadata();
        someMetadata.addNodeToHashRing(node);
        someMetadata.addNodeToHashRing(otherNode);
        server.replaceAllServerMetadata(someMetadata);

        utils.stall(2);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_7.cfg", true);
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

    @Disabled
    @Test
    public void serverReturnsNotResponsibleWithMetadataWhenGet() {
        // same as above

        int port = 50013;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/50013");
        server.clearStorage();
        server.start();

        // we have to add a lot of metadata because it only returns not responsible on get when it's not in the chain
        AllServerMetadata someMetadata = new AllServerMetadata();
        List<ECSNode> nodes = utils.generateECSNodes(port);
        nodes.forEach(someMetadata::addNodeToHashRing);

        server.replaceAllServerMetadata(someMetadata);

        utils.stall(1);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_8.cfg", true);
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
        ports.forEach(port -> asm.addNodeToHashRing(new ECSNode("localhost", port, null, null, false, false)));
        servers.forEach(s -> s.replaceAllServerMetadata(asm));
        servers.forEach(KVServer::start);

        utils.stall(2);

        KVInternalStore client = new KVInternalStore(asm);
        IKVMessage response = null;
        Exception ex = null;

        try {
            response = client.put("something", "else", 0);
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
        ports.forEach(port -> asm.addNodeToHashRing(new ECSNode("localhost", port, null, null, false, false)));
        servers.forEach(s -> s.replaceAllServerMetadata(asm));
        servers.forEach(KVServer::start);

        utils.stall(2);

        KVInternalStore client = new KVInternalStore(asm);
        IKVMessage response = null;
        Exception ex = null;

        try {
            response = client.put("something", "else", 0);
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
        ports.forEach(port -> asm.addNodeToHashRing(new ECSNode("localhost", port, null, null, false, false)));
        servers.forEach(s -> s.replaceAllServerMetadata(asm));
        servers.forEach(KVServer::start);

        utils.stall(2);

        KVInternalStore client = new KVInternalStore(asm);
        IKVMessage response = null;
        Exception ex = null;

        try {
            client.put("something", "else", 0);
            response = client.put("something", null, 0);
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

//    @Test
//    public void testShutdown() {
//        int port = 50025;
//
//        // initialize original server
//        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/50025");
//        server.clearStorage();
//        server.start();
//
//        // we have to add a lot of metadata because it only returns not responsible on get when it's not in the chain
//        AllServerMetadata someMetadata = new AllServerMetadata();
//        ECSNode node = new ECSNode("localhost", port, null, null, false, false);
//        someMetadata.addNode(node);
//        server.replaceAllServerMetadata(someMetadata);
//
//        server.replaceAllServerMetadata(someMetadata);
//
//        utils.stall(1);
//
//        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_12.cfg");
//        IServerMessage response = null;
//        Exception ex = null;
//
//        try {
//            response = kvClient.shutDown("localhost", port);
//        } catch (Exception e) {
//            ex = e;
//        }
//
//        assertTrue(false);
//        assertNull(ex);
////        assertSame(IKVMessage.StatusType.S, response.getStatus());
//        assertEquals(new Gson().toJson(someMetadata, AllServerMetadata.class), response.getKey());
//
//    }

//    @Test
//    public void helperLOL() {
//        KVStore store = new KVStore("localhost", 50010);
//        KVStore otherStore = new KVStore("localhost", 50010);
//
//        try {
//            otherStore.connect("localhost", 50010);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        IServerMessage message = store.shutDown("localhost", 50010);
//
//        System.out.println(message);
//        assertTrue(false);
//    }

    @Test
    public void heartbeat() {
        int port = 50033;

        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        ECSNode node = new ECSNode("localhost", port, "0".repeat(32), "F".repeat(32), true, false);
        AllServerMetadata asm = new AllServerMetadata();
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);

        server.clearStorage();
        server.start();
        utils.stall(2);

        KVStore kvClient = new KVStore("./src/test/resources/servers_kv_hb.cfg");
        IServerMessage response = null;
        Exception ex = null;
        try {
            response = kvClient.sendHeartbeat(node.getHost(), node.getPort());
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IServerMessage.StatusType.SERVER_HEARTBEAT, response.getStatus());
        assertEquals("Hello", response.getKey());
    }
}
