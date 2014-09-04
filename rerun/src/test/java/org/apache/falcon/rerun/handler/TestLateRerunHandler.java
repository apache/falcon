/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.rerun.handler;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LateArrival;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;

/**
 * Tests org.apache.falcon.rerun.handler.LateRerunHandler.
 */
public class TestLateRerunHandler {

    @Test
    public void testFeedCutOff() throws FalconException {
        Feed feed = new Feed();
        LateArrival lateArrival = new LateArrival();
        lateArrival.setCutOff(Frequency.fromString("days(1)"));
        feed.setLateArrival(lateArrival);
        String nm = "2013-10-01T12:00Z";
        Date cutOff = LateRerunHandler.getCutOffTime(feed, nm);
        Assert.assertEquals(EntityUtil.parseDateUTC("2013-10-02T12:00Z"), cutOff);

        lateArrival.setCutOff(Frequency.fromString("days(10000000)"));
        cutOff = LateRerunHandler.getCutOffTime(feed, nm);
        Assert.assertTrue(cutOff.after(new Date()));
    }

    /**
     * This test checks that LateData Handler is invoked only if LateArrival
     * is configured for a feed.
     */
    @Test
    public void testFeedLateArrivalCheck() throws FalconException {
        Feed feed = new Feed();
        String nominalTime = "2013-10-01T12:00Z";
        Date cutOff = LateRerunHandler.getCutOffTime(feed, nominalTime);
        Assert.assertEquals(cutOff, new Date(0));

    }
}
