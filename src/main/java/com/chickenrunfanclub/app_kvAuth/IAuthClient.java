package com.chickenrunfanclub.app_kvAuth;

import com.chickenrunfanclub.client.KVCommInterface;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IAuthMessage;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.IServerMessage;

public interface IAuthClient {

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

    public IAuthMessage add(String username, String password) throws Exception;

    public IAuthMessage authenticate(String username, String password) throws Exception;

}
