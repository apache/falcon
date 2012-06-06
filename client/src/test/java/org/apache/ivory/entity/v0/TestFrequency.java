package org.apache.ivory.entity.v0;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestFrequency {

    public void testValidFrequency() {
        String freqStr = "minutes(10)";
        Frequency freq = Frequency.fromString(freqStr);
        Assert.assertEquals(freq.getTimeUnit().name(), "minutes");
        Assert.assertEquals(freq.getFrequency(), 10);
    }
}
