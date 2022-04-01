package com.chickenrunfanclub;

import com.chickenrunfanclub.ecs.ECSNode;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestUtils {

    public TestUtils() {}

    public void stall(int seconds) {
        long start = System.currentTimeMillis();
        long end = start + seconds * 1000L;
        while (System.currentTimeMillis() < end) {}
    }

    public List<ECSNode> generateECSNodes(int port) {
        return IntStream.range(port, port + 10)
                .mapToObj(p -> new ECSNode("localhost", p, "A".repeat(32), "F".repeat(32), false, false))
                .collect(Collectors.toList());
    }
}
