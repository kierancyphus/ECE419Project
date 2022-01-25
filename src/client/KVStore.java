package client;

import shared.messages.IKVMessage;
import shared.messages.KVMessage;

public class KVStore implements KVCommInterface {
    String storePath;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {

    }

    @Override
    public void connect() throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void disconnect() {
        // TODO Auto-generated method stub
    }

    @Override
    public IKVMessage put(String key, String value) throws Exception {
        return new KVMessage(key, value, IKVMessage.StatusType.FAILED);
    }

    @Override
    public IKVMessage get(String key) throws Exception {
        return new KVMessage(key, null, IKVMessage.StatusType.FAILED);
    }

}
