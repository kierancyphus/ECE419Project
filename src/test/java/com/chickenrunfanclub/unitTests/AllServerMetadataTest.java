package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.Hasher;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AllServerMetadataTest {

    @Test
    public void serverMetadataPopulatesOnInit() {
        String filepath = "./src/test/resources/servers.cfg";
        AllServerMetadata metadata = new AllServerMetadata(filepath);
        Map<String, ECSNode> map = metadata.getHashToServer();
        Set<String> configs = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            configs.add("localhost 5000" + i);
        }
//        System.out.println(map);
        map.forEach((key, node) -> assertTrue(configs.contains(node.getHost() + " " + node.getPort())));
    }

    @Test
    public void findServerResponsibleOnGetReturnsTail() {
        String filepath = "./src/test/resources/servers.cfg";
        AllServerMetadata allServerMetadata = new AllServerMetadata(filepath);
        System.out.println("I am here!");
        ECSNode responsible = allServerMetadata.findServerResponsible("testkey", true);
        assertEquals(responsible.getRangeStart(), "36B8FE2C775706C80D50D3ADAACF741E");
    }

    @Test
    public void findServerResponsibleOnPutReturnsHead() {
        String filepath = "./src/test/resources/servers.cfg";
        AllServerMetadata allServerMetadata = new AllServerMetadata(filepath);
        ECSNode responsible = allServerMetadata.findServerResponsible("testkey", false);
        assertEquals(responsible.getRangeStart(), "104867805ED53D3F6233E69038C8F332");
    }

    // should really have another test here showing that we initialize them properly (e.g. closest hashes are together)
}
