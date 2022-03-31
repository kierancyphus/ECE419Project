package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvECS.ECSClient;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ECSTest {
    private static final TestUtils utils = new TestUtils();
    static ECSClient ecs;
    static int numServer;

    public ECSTest() throws IOException, InterruptedException, KeeperException {
    }

    @BeforeAll
    static void init() throws Exception {
        ecs = new ECSClient("src/test/java/com/chickenrunfanclub/unitTests/ecs.config.txt", "LRU", 5000, 50000);
        ecs.start();
        ecs.removeAllNodes();
        // ecs.shutdownServers();
        ecs = new ECSClient("src/test/java/com/chickenrunfanclub/unitTests/ecs.config.txt", "LRU", 5000, 50000);
        // numServer = ecs.getNumServers();
    }

    @Test
    public void addNode() throws Exception {
        numServer = ecs.getNumServers();
        assertSame(numServer, ecs.zookeeperNodes());
        ecs.addNode("LRU", 100);
        utils.stall(3);
        ecs.start();
        assertSame(1, ecs.getNumServers() - numServer);
        assertSame(1, ecs.zookeeperNodes() - numServer);
    }

    @Test
    public void addNodes() throws Exception {
        numServer = ecs.getNumServers();
        assertSame(numServer, ecs.zookeeperNodes());
        ecs.addNodes(2);
        utils.stall(3);
        ecs.start();
        assertSame(2, ecs.getNumServers() - numServer);
        assertSame(2, ecs.zookeeperNodes() - numServer);
    }

    @Test
    public void removeNodes() throws Exception {
        numServer = ecs.getNumServers();
        ecs.removeNode(0);
        assertSame(1, numServer - ecs.getNumServers());
        assertSame(1, numServer - ecs.zookeeperNodes());
    }

    @AfterAll
    static void shutdown() throws Exception {
        ecs.shutdown();
    }
}
