package com.chickenrunfanclub.app_kvServer.kvCache;

public interface IKVCache {
    boolean inCache(String key);

    String get(String key);

    void put(String key, String value);

    int getCacheSize();

    void clear();
}
