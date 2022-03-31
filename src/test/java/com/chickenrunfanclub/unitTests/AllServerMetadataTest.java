package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.ecs.ECSNode;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AllServerMetadataTest {

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
        metadata.addNode(node);

        assertEquals(1, metadata.getAllNodes().size());
        assertEquals(node, metadata.findServerResponsible("anything really"));
    }

    @Test
    public void secondNodeAddition() {
        AllServerMetadata metadata = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", 50000);
        ECSNode otherNode = new ECSNode("localhost", 50001);
        metadata.addNode(node);
        metadata.addNode(otherNode);

        assertEquals(2, metadata.getAllNodes().size());
        assertEquals(otherNode, metadata.findServerResponsible("anything really"));
    }

    @Test
    public void removeNodeToSingleNode() {
        AllServerMetadata metadata = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", 50000);
        ECSNode otherNode = new ECSNode("localhost", 50001);
        metadata.addNode(node);
        metadata.addNode(otherNode);
        metadata.removeNode(otherNode);

        assertEquals(1, metadata.getAllNodes().size());
        assertEquals(node, metadata.findServerResponsible("anything really"));
    }

    @Test
    public void removeNodeToLongerRing() {
        AllServerMetadata metadata = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", 50000);
        ECSNode otherNode = new ECSNode("localhost", 50001);
        ECSNode thirdNode = new ECSNode("localhost", 50002);
        metadata.addNode(node);
        metadata.addNode(otherNode);
        metadata.addNode(thirdNode);
        metadata.removeNode(thirdNode);

        assertEquals(2, metadata.getAllNodes().size());
        assertEquals(otherNode, metadata.findServerResponsible("anything really"));
    }

    // should really have another test here showing that we initialize them properly (e.g. closest hashes are together)
}
