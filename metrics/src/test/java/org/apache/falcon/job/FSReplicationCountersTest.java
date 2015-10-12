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

package org.apache.falcon.job;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test for FS Replication Counters.
 */
public class FSReplicationCountersTest {
    private List<String> countersList = new ArrayList<String>();
    private final String[] countersArgs = new String[] { "TIMETAKEN:5000", "BYTESCOPIED:1000L", "COPY:1" };

    @BeforeClass
    public void setUp() throws Exception {
        for (String counters : countersArgs) {
            String countersKey = counters.split(":")[0];
            countersList.add(countersKey);
        }
    }

    @Test
    public void testObtainJobCounters() throws Exception {
        for (String counters : countersArgs) {
            String countersKey = counters.split(":")[0];
            Assert.assertEquals(countersKey, ReplicationJobCountersList.getCountersKey(countersKey).getName());
        }

        Assert.assertEquals(countersArgs.length, ReplicationJobCountersList.values().length);
    }
}
