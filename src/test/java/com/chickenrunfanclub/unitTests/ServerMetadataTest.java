package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.ecs.ECSNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ServerMetadataTest {
    @Test
    public void inRange() {
        String startRange = "AA";
        String endRange = "BB";
        String hash = "AB";
        ECSNode ECSNode = new ECSNode("host", 1, startRange, endRange, false, false);

        assertTrue(ECSNode.inRange(hash));
    }

    @Test
    public void inRangeLowerBound() {
        String startRange = "AA";
        String endRange = "BB";
        String hash = "AA";
        ECSNode ECSNode = new ECSNode("host", 1, startRange, endRange, false, false);

        assertTrue(ECSNode.inRange(hash));
    }

    @Test
    public void inRangeUpperBoundNotInclusive() {
        String startRange = "AA";
        String endRange = "BB";
        String hash = "BB";
        ECSNode ECSNode = new ECSNode("host", 1, startRange, endRange, false, false);

        assertFalse(ECSNode.inRange(hash));
    }

    @Test
    public void inRangeWraps() {
        String startRange = "DD";
        String endRange = "22";
        String hashHigh = "EE";
        String hashLow = "11";
        ECSNode ECSNode = new ECSNode("host", 1, startRange, endRange, false, false);

        assertTrue(ECSNode.inRange(hashHigh));
        assertTrue(ECSNode.inRange(hashLow));
    }


}
