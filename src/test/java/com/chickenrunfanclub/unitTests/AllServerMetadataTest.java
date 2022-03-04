package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import org.junit.jupiter.api.Test;

public class AllServerMetadataTest {

    @Test
    public void something() {
        String filepath = "./src/test/resources/servers.cfg";
        AllServerMetadata metadata = new AllServerMetadata(filepath);
    }
}
