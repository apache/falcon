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

package org.apache.falcon.lifecycle;


import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.HsqldbTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

/**
 * Integration test for Datasource Update.
 */

public class DatasourceUpdateIT {
    public static final Logger LOG =  LoggerFactory.getLogger(DatasourceUpdateIT.class);

    private static final String DATASOURCE_NAME_KEY = "datasourcename";

    @BeforeClass
    public void setUp() throws Exception {
        HsqldbTestUtils.start();
        HsqldbTestUtils.createSqoopUser("sqoop_user", "sqoop");
        HsqldbTestUtils.createSqoopUser("sqoop_user2", "sqoop");
        HsqldbTestUtils.changeSAPassword("sqoop");
        HsqldbTestUtils.createAndPopulateCustomerTable();

        TestContext.cleanupStore();
        TestContext.prepare();
    }

    @AfterClass
    public void tearDown() throws Exception {
        HsqldbTestUtils.tearDown();
        FileUtils.deleteDirectory(new File("../localhost/"));
        FileUtils.deleteDirectory(new File("localhost"));
    }

    @Test
    public void testHSql() throws Exception {
        Assert.assertEquals(4, HsqldbTestUtils.getNumberOfRows());
    }

    public void testDatasourceUpdate() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);
        LOG.info("entity -submit -type cluster -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        // Make a new datasource name into the overlay so that DATASOURCE_TEMPLATE1 and FEED_TEMPLATE3
        // are populated  with the same datasource name
        String dsName = "datasource-test-1";
        overlay.put(DATASOURCE_NAME_KEY, dsName);
        filePath = TestContext.overlayParametersOverTemplate(TestContext.DATASOURCE_TEMPLATE1, overlay);
        LOG.info("Submit datatsource entity {} via entity -submit -type datasource -file {}", dsName, filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type datasource -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE3, overlay);
        LOG.info("Submit feed with datasource {} via entity -submitAndSchedule -type feed -file {}", dsName, filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));

        // instantiate another datasource with same name but different end point
        overlay.put(DATASOURCE_NAME_KEY, dsName);
        filePath = TestContext.overlayParametersOverTemplate(TestContext.DATASOURCE_TEMPLATE5, overlay);
        LOG.info("update datasource {} via -update -type datasource -file {}", dsName, filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -update -type datasource -file " + filePath));
    }

}
