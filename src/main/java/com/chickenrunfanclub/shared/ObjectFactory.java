package com.chickenrunfanclub.shared;

import com.chickenrunfanclub.app_kvClient.IKVClient;
import com.chickenrunfanclub.app_kvClient.KVClient;
import com.chickenrunfanclub.app_kvServer.IKVServer;
import com.chickenrunfanclub.app_kvServer.KVServer;

public final class ObjectFactory {
    /*
     * Creates a KVClient object for auto-testing purposes
     */
    public static IKVClient createKVClientObject() {
        return new KVClient("./src/test/resources/servers.cfg");
    }

    /*
     * Creates a KVServer object for auto-testing purposes
     */
    public static IKVServer createKVServerObject(int port, int cacheSize, String strategy) {
        return new KVServer(port, cacheSize, strategy);
    }
}