package com.chickenrunfanclub.app_kvServer.kvCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache implements IKVCache{
    private int cacheSize;
    private LinkedHashMap<String, String> cache;

    public LRUCache(int maxSize){
        this.cacheSize = maxSize;
        LinkedHashMap<String, String> cache = new LinkedHashMap<String, String>(cacheSize, (float) 1, true) {
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
        if (value==null){
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
