package com.chickenrunfanclub.unitTests;

import com.chickenrunfanclub.shared.ServerMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ServerMetadataTest {
    @Test
    public void inRange() {
        String startRange = "AA";
        String endRange = "BB";
        String hash = "AB";
        ServerMetadata serverMetadata = new ServerMetadata("host", 1, startRange, endRange, false, false);

        assertTrue(serverMetadata.inRange(hash));
    }

    @Test
    public void inRangeLowerBound() {
        String startRange = "AA";
        String endRange = "BB";
        String hash = "AA";
        ServerMetadata serverMetadata = new ServerMetadata("host", 1, startRange, endRange, false, false);

        assertTrue(serverMetadata.inRange(hash));
    }

    @Test
    public void inRangeUpperBoundNotInclusive() {
        String startRange = "AA";
        String endRange = "BB";
        String hash = "BB";
        ServerMetadata serverMetadata = new ServerMetadata("host", 1, startRange, endRange, false, false);

        assertFalse(serverMetadata.inRange(hash));
    }

    @Test
    public void inRangeWraps() {
        String startRange = "DD";
        String endRange = "22";
        String hashHigh = "EE";
        String hashLow = "11";
        ServerMetadata serverMetadata = new ServerMetadata("host", 1, startRange, endRange, false, false);

        assertTrue(serverMetadata.inRange(hashHigh));
        assertTrue(serverMetadata.inRange(hashLow));
    }


}
