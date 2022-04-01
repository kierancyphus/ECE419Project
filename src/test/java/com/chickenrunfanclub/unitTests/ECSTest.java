package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvECS.ECSClient;
import com.chickenrunfanclub.client.KVStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IServerMessage;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        System.out.println("wtf");
        utils.stall(3);
        System.out.println("hello");
        ecs.start();
        System.out.println("general kenobi");
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


    @Test
    public void heartbeat() throws Exception {
        Exception ex = null;
        ecs.addNodes(1);
        utils.stall(3);
        ecs.start();
        AllServerMetadata metadata = ecs.getMetadata();
        List<ECSNode> runningNodes = metadata.getAllNodesByStatus(ECSNode.ECSNodeFlag.START);
        assertSame(1, runningNodes.size());
        for (ECSNode node : runningNodes) {
            try {
                KVStore client = new KVStore(node.getHost(), node.getPort());
                IServerMessage hbResponse = client.sendHeartbeat(node.getHost(), node.getPort());

            } catch (Exception e) {
                ex = e;
                e.printStackTrace();
            }
        }
        assertNull(ex);
    }

    @AfterAll
    static void shutdown() throws Exception {
        ecs.shutdown();
    }
}
