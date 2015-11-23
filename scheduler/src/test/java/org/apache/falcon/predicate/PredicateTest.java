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
package org.apache.falcon.predicate;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.notification.service.event.TimeElapsedEvent;
import org.apache.falcon.state.EntityID;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests the predicate class.
 */
public class PredicateTest {

    @Test
    public void testPredicateFromEvent() throws FalconException {
        Process process = new Process();
        process.setName("test");
        DateTime now = DateTime.now();
        TimeElapsedEvent te = new TimeElapsedEvent(new EntityID(process), now, now, now);
        Predicate.getPredicate(te);
    }

    @Test
    public void testComparison() {
        Predicate firstPredicate = Predicate.createTimePredicate(100, 200, 50);
        Predicate secondPredicate = Predicate.createTimePredicate(1000, 2000, 50);
        Predicate thirdPredicate = Predicate.createTimePredicate(100, 200, -1);

        Assert.assertFalse(firstPredicate.evaluate(secondPredicate));
        Assert.assertFalse(secondPredicate.evaluate(thirdPredicate));
        //With "ANY" type
        Assert.assertTrue(firstPredicate.evaluate(thirdPredicate));
    }
}
