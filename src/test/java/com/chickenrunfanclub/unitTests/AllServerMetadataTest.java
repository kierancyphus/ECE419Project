package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.ecs.ECSNode;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AllServerMetadataTest {

    @Test
    public void serverMetadataPopulatesOnInit() {
        String filepath = "./src/test/resources/servers.cfg";
        AllServerMetadata metadata = new AllServerMetadata(filepath);
        Map<String, ECSNode> map = metadata.getHashToServer();
        Set<String> configs = new HashSet<>(Arrays.asList("localhost 50000", "localhost 50001", "localhost 50002"));
//        System.out.println(map);
        map.forEach((key, node) -> assertTrue(configs.contains(node.getHost() + " " + node.getPort())));
    }

    // should really have another test here showing that we initialize them properly (e.g. closest hashes are together)
}
