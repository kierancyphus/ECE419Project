package com.chickenrunfanclub.app_kvServer;

import com.chickenrunfanclub.shared.ServerMetadata;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

    /**
     * Lock the KVServer for write operations.
     * */
    public void lockWrite();


    /**
     * Unlock the KVServer for write operations.
     * */
    public void unLockWrite();

    /**
     * Transfer a subset (range) of the KVServer’s data to another KVServer (reallocation before removing this server or adding a new KVServer to the ring);
     * send a notification to the ECS, if data transfer is completed. Doesn't update the current metadata (assumed to already happen in another operation)
     * */
    public boolean moveData(ServerMetadata metaData);

    /**
     * Update the metadata repository of this server
     * */
    public void updateMetadata(ServerMetadata serverMetadata);

}
