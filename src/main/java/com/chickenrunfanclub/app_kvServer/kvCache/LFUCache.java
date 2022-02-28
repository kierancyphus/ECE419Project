package com.chickenrunfanclub.app_kvServer.kvCache;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

public class LFUCache implements IKVCache{
    private HashMap<String, String> keyValue;
    private HashMap<String, Integer> counts;
    private HashMap<Integer, LinkedHashSet<String>> list;

    private int cacheSize;
    private int minimum = -1;


    public LFUCache(int maxSize){
        this.cacheSize = maxSize;
        keyValue = new HashMap<>();
        counts = new HashMap<>();
        list = new HashMap<>();
        list.put(1, new LinkedHashSet<String>());
    }

    @Override
    public synchronized boolean inCache(String key) {
        return keyValue.containsKey(key);
    }

    @Override
    public synchronized String get(String key) {
        if (keyValue.containsKey(key)){
            // increase the reference counter
            int count = counts.get(key);
            counts.put(key, count+1);
            // remove the word from the counter-wordlist
            list.get(count).remove(key);
            // if there are no more values referenced at level count, then increase min
            if (list.get(count).size() == 0 && count == minimum){
                minimum++;
            }
            // if count hasn't got to this level, create new linkedhashset
            if (!list.containsKey(count+1)){
                LinkedHashSet<String> newList = new LinkedHashSet<>();
                list.put(count + 1, newList);
            }

            // add word to number of word lists
            list.get(count+1).add(key);

            return keyValue.get(key);
        }else{
            return null;
        }
    }

    @Override
    public synchronized void put(String key, String value) {
        if (value == null){
            // remove reference from all stores
            keyValue.remove(key);
            int count = counts.get(key);
            counts.remove(key);
            list.get(count).remove(key);
        } else{
            // if in the cache, update the key and counts by calling get()
            if(keyValue.containsKey(key)){
                keyValue.put(key, value);
                get(key);
            } else{
                // if size has exceeded, choose the evicted by choosing minimum freq
                if (keyValue.size() >= cacheSize){
                    String evict = list.get(minimum).iterator().next();
                    keyValue.remove(evict);
                    counts.remove(evict);
                }
                keyValue.put(key, value);
                counts.put(key, 1);
                minimum = 1;
                list.get(1).add(key);
            }
        }
    }

    @Override
    public synchronized int getCacheSize() {
        return cacheSize;
    }

    @Override
    public synchronized void clear() {
        keyValue.clear();
        counts.clear();
        list.clear();
        list.put(1, new LinkedHashSet<String>());
    }
}
