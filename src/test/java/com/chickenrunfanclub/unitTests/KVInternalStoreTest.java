package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVInternalStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.IServerMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class KVInternalStoreTest {
    private static final TestUtils utils = new TestUtils();

    @Test
    public void updateAllMetadata() {
        int port = 50042;

        // initialize original server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        AllServerMetadata asm = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", port, null, null, false, false);
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);

        server.clearStorage();
        server.start();
        server.updateServerStopped(false);
        utils.stall(3);

        KVInternalStore client = new KVInternalStore(asm);
        IServerMessage response = null;
        Exception ex = null;
        try {
            // have to lock and unlock here so we can delete value first
            response = client.updateAllMetadata(asm, "localhost", port);
        } catch (Exception e) {
            System.out.println("Caught exception");
            e.printStackTrace();
            ex = e;
        }
        assertNull(ex);
        assertSame(IServerMessage.StatusType.SERVER_UPDATE_ALL_METADATA, response.getStatus());
    }
}
