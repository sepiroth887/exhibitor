package com.netflix.exhibitor.core.backup.azure;

import junit.framework.Assert;
import org.joda.time.Duration;
import org.testng.annotations.Test;


public class TimeTest {

    @Test
    public void testDuration(){
        Duration now = Duration.millis(System.currentTimeMillis());
        Duration older = Duration.millis(System.currentTimeMillis() - 2 * 60 * 1000);

        Duration retention = Duration.parse("PT60S");
        Assert.assertTrue(now.minus(older).isLongerThan(retention));
    }
}
