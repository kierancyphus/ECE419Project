package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.apiGateway.ApiGateway;
import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.KVMessage;
import com.chickenrunfanclub.shared.messages.ServerMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class AllServerMetadataTest {

    private final TestUtils utils = new TestUtils();

    @Test
    public void serverMetadataPopulatesOnInit() {
        String filepath = "./src/test/resources/servers.cfg";
        AllServerMetadata metadata = new AllServerMetadata(filepath);
        Map<String, ECSNode> map = metadata.getHashToServer();
        Set<String> configs = new HashSet<>(Arrays.asList("localhost 50010", "localhost 50011", "localhost 50012"));
        map.forEach((key, node) -> assertTrue(configs.contains(node.getHost() + " " + node.getPort())));
    }

    @Test
    public void successfulFirstNodeAddition() {
        AllServerMetadata metadata = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", 50000);
        metadata.addNodeToHashRing(node);

        assertEquals(1, metadata.getAllNodes().size());
        assertEquals(node, metadata.findServerResponsible("anything really", false));
    }

    @Test
    public void secondNodeAddition() {
        AllServerMetadata metadata = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", 50000);
        ECSNode otherNode = new ECSNode("localhost", 50001);
        metadata.addNodeToHashRing(node);
        metadata.addNodeToHashRing(otherNode);

        assertEquals(2, metadata.getAllNodes().size());
        assertEquals(otherNode, metadata.findServerResponsible("anything really", false));
    }

    @Test
    public void removeNodeToSingleNode() {
        AllServerMetadata metadata = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", 50000);
        ECSNode otherNode = new ECSNode("localhost", 50001);
        metadata.addNodeToHashRing(node);
        metadata.addNodeToHashRing(otherNode);
        metadata.removeNodeFromHashRing(otherNode);

        assertEquals(1, metadata.getAllNodes().size());
        assertEquals(node, metadata.findServerResponsible("anything really", false));
    }

    @Test
    public void removeNodeToLongerRing() {
        AllServerMetadata metadata = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", 50000);
        ECSNode otherNode = new ECSNode("localhost", 50001);
        ECSNode thirdNode = new ECSNode("localhost", 50002);
        metadata.addNodeToHashRing(node);
        metadata.addNodeToHashRing(otherNode);
        metadata.addNodeToHashRing(thirdNode);
        metadata.removeNodeFromHashRing(thirdNode);

        assertEquals(2, metadata.getAllNodes().size());
        assertEquals(otherNode, metadata.findServerResponsible("anything really", false));
    }

    @Test
    public void findServerResponsibleOnGetReturnsTail() {
        AllServerMetadata metadata = new AllServerMetadata();

        ECSNode node = new ECSNode("localhost", 50000);
        ECSNode otherNode = new ECSNode("localhost", 50001);
        ECSNode thirdNode = new ECSNode("localhost", 50002);
        metadata.addNodeToHashRing(node);
        metadata.addNodeToHashRing(otherNode);
        metadata.addNodeToHashRing(thirdNode);

        ECSNode responsible = metadata.findServerResponsible("testkey", true);
        assertEquals(otherNode, responsible);
    }

    @Test
    public void findServerResponsibleOnPutReturnsHead() {
        AllServerMetadata metadata = new AllServerMetadata();

        ECSNode node = new ECSNode("localhost", 50000);
        ECSNode otherNode = new ECSNode("localhost", 50001);
        ECSNode thirdNode = new ECSNode("localhost", 50002);
        metadata.addNodeToHashRing(node);
        metadata.addNodeToHashRing(otherNode);
        metadata.addNodeToHashRing(thirdNode);

        ECSNode responsible = metadata.findServerResponsible("testkey", false);
        assertEquals(thirdNode, responsible);
    }

    @Test
    public void createsProperGetReplicationChain() {
        // make big hash ring
        AllServerMetadata metadata = new AllServerMetadata();
        List<ECSNode> nodes = utils.generateECSNodes(50000);
        nodes.forEach(metadata::addNodeToHashRing);

        List<ECSNode> getChain = metadata.findGetServersResponsible("key");

        // precomputed replication chain is 5000[467]
        assertTrue(getChain.contains(nodes.get(4)));
        assertTrue(getChain.contains(nodes.get(6)));
        assertTrue(getChain.contains(nodes.get(7)));
    }

    @Test
    public void broadcastMetadata() {
        int gatewayPort = 50050;

        // initialize a bunch of storage servers
        List<Integer> ports = Arrays.asList(50051, 50052);
        List<KVServer> servers = ports
                .stream()
                .map(p -> new KVServer(p, 10, "FIFO", "./testStore/KVServer/" + p))
                .collect(Collectors.toList());
        servers.forEach(KVServer::clearStorage);
        AllServerMetadata asm = new AllServerMetadata();
        ports.forEach(port -> asm.addNodeToHashRing(new ECSNode("localhost", port, null, null, false, false)));

        // initialize api gateway
        ApiGateway gateway = new ApiGateway(gatewayPort);
        ECSNode gatewayNode = new ECSNode("localhost", gatewayPort);
        asm.setGateway(gatewayNode);

        // set everything to have empty metadata initially and start
        servers.forEach(s -> s.replaceAllServerMetadata(new AllServerMetadata()));
        servers.forEach(KVServer::start);
        gateway.replaceAllServerMetadata(new AllServerMetadata());
        gateway.start();

        utils.stall(2);

        // broadcast metadata and check to make sure not empty on servers and gateway
        asm.broadcastMetadata();

        assertFalse(gateway.getAllServerMetadata().isEmpty());
        assertFalse(servers.get(0).getAllMetadata().isEmpty());
        assertFalse(servers.get(1).getAllMetadata().isEmpty());
    }

    @Disabled
    @Test
    public void helper() throws Exception {
//        AllServerMetadata asm = new AllServerMetadata();
////        ECSNode node = new ECSNode("localhost", 50000, null, null, false, false);
//        ECSNode otherNode = new ECSNode("localhost", 50002, null, null, false, false);
////        asm.addNodeToHashRing(node);
//        asm.addNodeToHashRing(otherNode);

        KVStore client = new KVStore("./src/test/resources/test.cfg", true);
        client.start("localhost", 50002);

        AllServerMetadata asm = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", 50002, null, null, false, false);
        asm.addNodeToHashRing(node);
        asm.broadcastMetadata();
//        client.updateAllMetadata(asm, "localhost", 50000);

        IKVMessage message = client.put("some key", "some value", "localhost", 50002, 0);
        IKVMessage getMessage = client.get("some key");

        System.out.println(message);
        System.out.println(getMessage);

        assertTrue(false);
    }
}
