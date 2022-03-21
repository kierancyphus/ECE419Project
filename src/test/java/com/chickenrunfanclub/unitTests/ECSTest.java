package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvECS.ECSClient;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ECSTest {
    private static final TestUtils utils = new TestUtils();
    static ECSClient ecs;

    static {
        try {
            ecs = new ECSClient("src/test/java/com/chickenrunfanclub/unitTests/ecs.config.txt", "LRU", 5000);
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public ECSTest() throws IOException, InterruptedException, KeeperException {
    }

    @BeforeAll
    static void init() throws Exception {
        ecs.removeAllNodes();
    }

    @Test
    public void zooInit() throws Exception {
        assertSame(4, ecs.getNodes().size());
    }

    @Test
    public void addNodes() throws Exception {
        ecs.addNodes(3, "LRU", 5000);
        System.out.println("hello22222");
        System.out.println(ecs.getNodes().size());
    }
}
