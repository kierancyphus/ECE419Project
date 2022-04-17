package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.TestUtils;
import com.chickenrunfanclub.apiGateway.ApiGateway;
import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.app_kvServer.KVServer;
import com.chickenrunfanclub.client.KVExternalStore;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class APIGatewayTest {

    private final static TestUtils utils = new TestUtils();

    @Test
    public void putSuccessWhenAuthenticated() {
        int port = 50025;
        int gatewayPort = 50026;

        // initialize kv server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        AllServerMetadata asm = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", port, null, null, false, false);
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);
        server.clearStorage();
        server.start();

        // initialize api gateway
        ApiGateway gateway = new ApiGateway(gatewayPort);
        gateway.replaceAllServerMetadata(asm);
        gateway.start();

        utils.stall(1);

        KVExternalStore client = new KVExternalStore("./src/test/resources/gateway.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            // have to lock and unlock here so we can delete value first
            client.setUsername("chicken");
            client.setPassword("runfanclub");
            response = client.put("key", "value", 0);
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.PUT_SUCCESS, response.getStatus());
    }

    @Test
    public void getSuccessWhenAuthenticated() {
        int port = 50027;
        int gatewayPort = 50028;

        // initialize kv server
        KVServer server = new KVServer(port, 10, "FIFO", "./testStore/KVServer/" + port);
        AllServerMetadata asm = new AllServerMetadata();
        ECSNode node = new ECSNode("localhost", port, null, null, false, false);
        asm.addNodeToHashRing(node);
        server.replaceAllServerMetadata(asm);
        server.clearStorage();
        server.start();

        // initialize api gateway
        ApiGateway gateway = new ApiGateway(gatewayPort);
        gateway.replaceAllServerMetadata(asm);
        gateway.start();

        utils.stall(1);

        KVExternalStore client = new KVExternalStore("./src/test/resources/gateway_2.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            // have to lock and unlock here so we can delete value first
            client.setUsername("chicken");
            client.setPassword("runfanclub");
            client.put("key", "value", 0);
            response = client.get("key");
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.GET_SUCCESS, response.getStatus());
    }

    @Test
    public void putGetSuccessWhenMultipleServers() {
        int gatewayPort = 50029;

        // start a bunch of storage servers
        List<Integer> ports = Arrays.asList(50030, 50031, 50032, 50033, 50034, 50035, 50036);
        List<KVServer> servers = ports
                .stream()
                .map(p -> new KVServer(p, 10, "FIFO", "./testStore/KVServer/" + p))
                .collect(Collectors.toList());
        servers.forEach(KVServer::clearStorage);

        AllServerMetadata asm = new AllServerMetadata();
        ports.forEach(port -> asm.addNodeToHashRing(new ECSNode("localhost", port, null, null, false, false)));
        servers.forEach(s -> s.replaceAllServerMetadata(asm));
        servers.forEach(KVServer::start);

        // initialize api gateway
        ApiGateway gateway = new ApiGateway(gatewayPort);
        gateway.replaceAllServerMetadata(asm);
        gateway.start();

        utils.stall(1);

        KVExternalStore client = new KVExternalStore("./src/test/resources/gateway_3.cfg");
        IKVMessage response = null;
        Exception ex = null;
        try {
            // have to lock and unlock here so we can delete value first
            client.setUsername("chicken");
            client.setPassword("runfanclub");
            client.put("key", "value", 0);
            response = client.get("key");
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
        assertSame(IKVMessage.StatusType.GET_SUCCESS, response.getStatus());
    }

}
