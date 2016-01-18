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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.HsqldbTestUtils;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

/**
 * Integration test for Feed Import.
 */

@Test
public class FeedImportIT {
    public static final Log LOG = LogFactory.getLog(HsqldbTestUtils.class.getName());

    @BeforeClass
    public void setUp() throws Exception {
        HsqldbTestUtils.start();
        HsqldbTestUtils.changeSAPassword("sqoop");
        HsqldbTestUtils.createAndPopulateCustomerTable();

        TestContext.cleanupStore();
        TestContext.prepare();
    }

    @AfterClass
    public void tearDown() throws Exception {
        HsqldbTestUtils.tearDown();
        FileUtils.deleteDirectory(new File("webapp/localhost/"));
        FileUtils.deleteDirectory(new File("localhost"));
    }

    @Test
    public void testFeedImportHSql() throws Exception {
        Assert.assertEquals(4, HsqldbTestUtils.getNumberOfRows());
    }

    @Test
    public void testSqoopImport() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);
        LOG.info("entity -submit -type cluster -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.DATASOURCE_TEMPLATE, overlay);
        LOG.info("entity -submit -type datasource -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type datasource -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE3, overlay);
        LOG.info("entity -submitAndSchedule -type feed -file " + filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file "
                + filePath));
    }

    @Test
    public void testSqoopImportDeleteDatasource() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);
        LOG.info("entity -submit -type cluster -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.DATASOURCE_TEMPLATE, overlay);
        LOG.info("entity -submit -type datasource -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type datasource -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE3, overlay);
        LOG.info("entity -submit -type feed -file " + filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type feed -file "
                + filePath));

        LOG.info("entity -delete -type datasource -name datasource-test");
        Assert.assertEquals(-1, TestContext.executeWithURL("entity -delete -type datasource -name datasource-test"));
    }
}
