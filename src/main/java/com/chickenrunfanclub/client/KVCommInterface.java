package com.chickenrunfanclub.client;

import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.IServerMessage;

public interface KVCommInterface {

    /**
     * Establishes a connection to the KV Server.
     *
     * @throws Exception if connection could not be established.
     */
    public void connect(String address, int port) throws Exception;

    /**
     * disconnects the client from the currently connected server.
     */
    public void disconnect();

    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    public IKVMessage put(String key, String value) throws Exception;

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return the value, which is indexed by the given key.
     * @throws Exception if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    public IKVMessage get(String key) throws Exception;

    public IServerMessage start(String address, int port) throws Exception;

    public IServerMessage stop(String address, int port) throws Exception;

    public IServerMessage shutDown(String address, int port) throws Exception;

    public IServerMessage lockWrite(String address, int port) throws Exception;

    public IServerMessage unlockWrite(String address, int port) throws Exception;

    public IServerMessage moveData(ECSNode metadata) throws Exception;

    public IServerMessage updateMetadata(ECSNode metadata) throws Exception;

}
