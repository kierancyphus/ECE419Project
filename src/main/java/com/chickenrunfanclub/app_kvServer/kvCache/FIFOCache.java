package com.chickenrunfanclub.app_kvServer.kvCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOCache implements IKVCache{
    private int cacheSize;
    private LinkedHashMap<String, String> cache;

    public FIFOCache(int maxSize){
        this.cacheSize = maxSize;
        this.cache = new LinkedHashMap<String, String>(cacheSize, (float) 1, false) {
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > cacheSize;
            }
        };
    }

    @Override
    public synchronized boolean inCache(String key) {
        return cache.containsKey(key);
    }

    @Override
    public synchronized String get(String key) {
        return cache.get(key);
    }

    @Override
    public synchronized void put(String key, String value) {
        if (value == null){
            cache.remove(key);
        } else {
            cache.put(key, value);
        }
    }

    @Override
    public synchronized int getCacheSize() {
        return cacheSize;
    }

    @Override
    public synchronized void clear() {
        cache.clear();
    }
}
