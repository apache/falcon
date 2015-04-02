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

package org.apache.falcon.catalog;

import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.HiveTestUtils;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * IT for catalog partition handler which is JMS message listener.
 */
@Test(groups = {"exhaustive"})
public class CatalogPartitionHandlerIT {
    private static final String METASTORE_URL = "thrift://localhost:49083";
    private static final String DB = "falcon_db";
    private static final String TABLE = "output_table";

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
    }

    // TODO : Enable this after oozie/hadoop config file changes
    @Test(enabled = false)
    public void testPartitionRegistration() throws Exception {
        TestContext context = newContext();

        HiveTestUtils.createDatabase(METASTORE_URL, DB);
        HiveTestUtils.createTable(METASTORE_URL, DB, TABLE, Arrays.asList("ds"));
        context.scheduleProcess();
        List<WorkflowJob> instances = OozieTestUtils.waitForProcessWFtoStart(context);
        OozieTestUtils.waitForInstanceToComplete(context, instances.get(0).getId());

        HCatPartition partition = HiveTestUtils.getPartition(METASTORE_URL, DB, TABLE, "ds", "2012-04-19");
        Assert.assertNotNull(partition);
    }

    private ThreadLocal<TestContext> contexts = new ThreadLocal<TestContext>();

    private TestContext newContext() {
        contexts.set(new TestContext());
        return contexts.get();
    }

    @AfterMethod
    public void cleanup() throws Exception {
        TestContext testContext = contexts.get();
        if (testContext != null) {
            OozieTestUtils.killOozieJobs(testContext);
        }

        contexts.remove();
    }
}
