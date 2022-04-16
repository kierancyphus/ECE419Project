package com.chickenrunfanclub.app_kvClient;

import com.chickenrunfanclub.client.KVCommInterface;

public interface IKVClient {
    /**
     * Creates a new connection to hostname:port
     *
     * @throws Exception when a connection to the server can not be established
     */
    public void newConnection(String hostname, int port) throws Exception;
}
