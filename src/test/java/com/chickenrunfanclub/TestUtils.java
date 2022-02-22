package com.chickenrunfanclub;

public class TestUtils {

    public TestUtils() {}

    public void stall(int seconds) {
        long start = System.currentTimeMillis();
        long end = start + seconds * 1000L;
        while (System.currentTimeMillis() < end) {}
    }
}
