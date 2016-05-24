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

package org.apache.falcon.resource;

import org.apache.falcon.FalconWebException;
import org.apache.falcon.resource.metadata.MetadataTestContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit tests for org.apache.falcon.resource.AbstractInstanceManager.
 */
public class InstanceManagerTest {

    private static final String PROCESS_NAME_PREFIX = "instance-search-test-process";
    private static final String PROCESS_NAME_1 = "instance-search-test-process-1";
    private static final String PROCESS_NAME_2 = "instance-search-test-process-2";
    private static final String NOMINAL_TIME_1 = "2015-01-01-01-00";
    private static final String NOMINAL_TIME_2 = "2015-01-02-01-00";
    private static final String NOMINAL_TIME_3 = "2015-01-03-01-00";

    private MetadataTestContext testContext;

    @BeforeClass
    public void setUp() throws Exception {
        testContext = new MetadataTestContext();
        testContext.setUp();
    }

    @AfterClass
    public void tearDown() throws Exception {
        testContext.tearDown();
    }

    @Test
    public void testInstanceSearch() throws Exception {
        // Note: all the following tests are based on entity name prefix PROCESS_NAME_PREFIX
        testContext.addProcessEntity(PROCESS_NAME_1);
        testContext.addInstance(PROCESS_NAME_1, NOMINAL_TIME_1, MetadataTestContext.SUCCEEDED_STATUS);
        testContext.addInstance(PROCESS_NAME_1, NOMINAL_TIME_2, MetadataTestContext.SUCCEEDED_STATUS);
        testContext.addInstance(PROCESS_NAME_1, NOMINAL_TIME_3, MetadataTestContext.RUNNING_STATUS);
        testContext.addProcessEntity(PROCESS_NAME_2);
        testContext.addInstance(PROCESS_NAME_2, NOMINAL_TIME_1, MetadataTestContext.FAILED_STATUS);
        testContext.addInstance(PROCESS_NAME_2, NOMINAL_TIME_2, MetadataTestContext.RUNNING_STATUS);

        // list all instances
        BaseInstanceManager instanceManager = new BaseInstanceManager();
        InstancesResult result;
        result = instanceManager.searchInstances("PROCESS", PROCESS_NAME_PREFIX, "", "", "", "", "", 0, 10);
        Assert.assertEquals(result.getInstances().length, 5);

        // running status
        result = instanceManager.searchInstances("PROCESS", PROCESS_NAME_PREFIX, "", "", "",
                MetadataTestContext.RUNNING_STATUS, "", 0, 10);
        Assert.assertEquals(result.getInstances().length, 2);

        // succeeded status
        result = instanceManager.searchInstances("PROCESS", PROCESS_NAME_PREFIX, "", "", "",
                MetadataTestContext.SUCCEEDED_STATUS, "", 0, 10);
        Assert.assertEquals(result.getInstances().length, 2);

        // failed status
        result = instanceManager.searchInstances("PROCESS", PROCESS_NAME_PREFIX, "", "", "",
                MetadataTestContext.FAILED_STATUS, "", 0, 10);
        Assert.assertEquals(result.getInstances().length, 1);

        // nominal time filter
        result = instanceManager.searchInstances("PROCESS", PROCESS_NAME_PREFIX, "", NOMINAL_TIME_2, "", "", "", 0, 10);
        Assert.assertEquals(result.getInstances().length, 3);
    }

    @Test(expectedExceptions = FalconWebException.class)
    public void test() {
        BaseInstanceManager instanceManager = new BaseInstanceManager();
        instanceManager.triageInstance("process", "random", "2014-05-07T00:00Z", "default");
    }

    private class BaseInstanceManager extends AbstractInstanceManager {}
}
