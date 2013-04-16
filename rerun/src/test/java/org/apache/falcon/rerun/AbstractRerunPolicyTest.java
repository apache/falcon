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
package org.apache.falcon.rerun;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.rerun.policy.AbstractRerunPolicy;
import org.apache.falcon.rerun.policy.ExpBackoffPolicy;
import org.apache.falcon.rerun.policy.PeriodicPolicy;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;

public class AbstractRerunPolicyTest {

    @Test
    public void TestGetDurationInMillis() throws FalconException {
        AbstractRerunPolicy policy = new AbstractRerunPolicy() {

            @Override
            public long getDelay(Frequency delay, Date nominaltime,
                                 Date cutOffTime) throws FalconException {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public long getDelay(Frequency delay, int eventNumber)
                    throws FalconException {
                // TODO Auto-generated method stub
                return 0;
            }
        };

        Frequency frequency = new Frequency("minutes(1)");
        Assert.assertEquals(policy.getDurationInMilliSec(frequency), 60000);
        frequency = new Frequency("minutes(15)");
        Assert.assertEquals(policy.getDurationInMilliSec(frequency), 900000);
        frequency = new Frequency("hours(2)");
        Assert.assertEquals(policy.getDurationInMilliSec(frequency), 7200000);
    }

    @Test
    public void TestExpBackoffPolicy() throws FalconException {
        AbstractRerunPolicy backoff = new ExpBackoffPolicy();
        long delay = backoff.getDelay(new Frequency("minutes(2)"), 2);
        Assert.assertEquals(delay, 480000);

        long currentTime = System.currentTimeMillis();
        delay = backoff.getDelay(new Frequency("minutes(2)"), new Date(
                currentTime - 1 * 4 * 60 * 1000), new Date(currentTime + 1 * 60
                * 60 * 1000));
        Assert.assertEquals(delay, 1 * 6 * 60 * 1000);

        currentTime = System.currentTimeMillis();
        delay = backoff.getDelay(new Frequency("minutes(1)"), new Date(
                currentTime - 1 * 9 * 60 * 1000), new Date(currentTime + 1 * 60
                * 60 * 1000));
        Assert.assertEquals(delay, 900000);
    }

    @Test
    public void TestPeriodicPolicy() throws FalconException, InterruptedException {
        AbstractRerunPolicy periodic = new PeriodicPolicy();
        long delay = periodic.getDelay(new Frequency("minutes(2)"), 2);
        Assert.assertEquals(delay, 120000);
        delay = periodic.getDelay(new Frequency("minutes(2)"), 5);
        Assert.assertEquals(delay, 120000);

        long currentTime = System.currentTimeMillis();
        //Thread.sleep(1000);
        delay = periodic.getDelay(new Frequency("minutes(3)"), new Date(
                currentTime), new Date(currentTime + 1 * 60
                * 60 * 1000));
        Assert.assertEquals(delay, 180000);
    }
}
