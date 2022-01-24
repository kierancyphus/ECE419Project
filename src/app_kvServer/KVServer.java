package app_kvServer;

import client.KVCommInterface;
import client.KVStore;
import shared.messages.IKVMessage;

public class KVServer extends Thread implements IKVServer {
    private String hostname;
    private int port;
    int cacheSize;
    // TODO: Change to enum
    String strategy;
    KVCommInterface store;

    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = strategy;
        this.store = new KVStore("localhost", port);
    }

    @Override
    public int getPort() {
        // TODO Auto-generated method stub
        return this.port;
    }

    @Override
    public String getHostname() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        // TODO Auto-generated method stub
        return IKVServer.CacheStrategy.None;
    }

    @Override
    public int getCacheSize() {
        // TODO Auto-generated method stub
        return this.cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
		IKVMessage response = this.store.get(key);
		return response.getValue();
    }

    @Override
    public void putKV(String key, String value) throws Exception {
		this.store.put(key, value);
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
		this.store.nukeStore();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
    }

    @Override
    public void kill() {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }
}
