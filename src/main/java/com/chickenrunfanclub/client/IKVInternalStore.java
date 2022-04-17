package com.chickenrunfanclub.client;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;
import com.chickenrunfanclub.ecs.ECSNode;
import com.chickenrunfanclub.shared.messages.IKVMessage;
import com.chickenrunfanclub.shared.messages.IServerMessage;

public interface IKVInternalStore extends IKVExternalStore {
    public IKVMessage moveDataPut(String key, String value, String host, int port);

    public IKVMessage forwardPut(String key, String value, int index, String host, int port);

    public IServerMessage start(String host, int port);

    public IServerMessage stop(String host, int port);

    public IServerMessage shutdown(String host, int port);

    public IServerMessage lockWrite(String host, int port);

    public  IServerMessage unlockWrite(String host, int port);

    public IServerMessage updateMetadata(ECSNode metadata, String host, int port);

    public IServerMessage updateAllMetadata(AllServerMetadata asm, String host, int port);

    public IServerMessage sendHeartbeat(String address, int port);
}
