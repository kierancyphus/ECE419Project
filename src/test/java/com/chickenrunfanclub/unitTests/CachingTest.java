package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.app_kvServer.kvCache.FIFOCache;
import com.chickenrunfanclub.app_kvServer.kvCache.LFUCache;
import com.chickenrunfanclub.app_kvServer.kvCache.LRUCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
public class CachingTest {

    @Test
    public void testLRUCache() {
        LRUCache cache = new LRUCache(2);
        cache.put("foo", "1");
        cache.put("bar", "2");
        assertSame(cache.get("foo"), "1");
        cache.put("baz", "3");
        assertSame(cache.get("bar"), null);
        assertTrue(cache.inCache("foo"));
        assertTrue(cache.inCache("baz"));
    }

    @Test
    public void testFIFOCache() {
        FIFOCache cache = new FIFOCache(2);
        cache.put("foo", "1");
        assertSame(cache.get("foo"), "1");
        cache.put("bar", "2");
        cache.put("baz", "3");
        assertSame(cache.get("foo"), null);
        assertTrue(cache.inCache("bar"));
        assertTrue(cache.inCache("baz"));

    }

    @Test
    public void testLFUCache() {
        LFUCache cache = new LFUCache(2);
        cache.put("foo", "1");
        cache.put("bar", "2");
        assertSame(cache.get("foo"), "1");
        cache.put("baz", "3");
        assertSame(cache.get("bar"), null);
        cache.put("baz", "4");
        assertSame(cache.get("baz"), "4");
        cache.put("baz", "5");
        cache.put("foo", "6");
        cache.put("bar", "1");

        assertTrue(cache.inCache("bar"));
        assertTrue(cache.inCache("baz"));
    }

}
