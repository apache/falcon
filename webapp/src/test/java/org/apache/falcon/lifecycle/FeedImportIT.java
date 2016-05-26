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
import org.apache.commons.io.IOUtils;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.HiveTestUtils;
import org.apache.falcon.util.HsqldbTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Integration test for Feed Import.
 */

@Test (enabled = false)
public class FeedImportIT {
    public static final Logger LOG =  LoggerFactory.getLogger(FeedImportIT.class);

    private static final String DATASOURCE_NAME_KEY = "datasourcename";
    private static final String METASTORE_URL = "thrift://localhost:49083";
    private static final String DATABASE_NAME = "SqoopTestDB";
    private static final String TABLE_NAME = "SqoopTestTable";

    private HCatClient client;
    private CatalogStorage storage;

    @BeforeClass
    public void setUp() throws Exception {
        HsqldbTestUtils.start();
        HsqldbTestUtils.createSqoopUser("sqoop_user", "sqoop");
        HsqldbTestUtils.changeSAPassword("sqoop");
        HsqldbTestUtils.createAndPopulateCustomerTable();

        TestContext.cleanupStore();
        TestContext.prepare();

        // setup hcat
        CurrentUser.authenticate(TestContext.REMOTE_USER);
        client = TestContext.getHCatClient(METASTORE_URL);

        HiveTestUtils.createDatabase(METASTORE_URL, DATABASE_NAME);
        List<String> partitionKeys = new ArrayList<>();
        partitionKeys.add("year");
        partitionKeys.add("month");
        partitionKeys.add("day");
        partitionKeys.add("hour");
        HiveTestUtils.createTable(METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionKeys);
    }

    @AfterClass
    public void tearDown() throws Exception {
        HsqldbTestUtils.tearDown();
        FileUtils.deleteDirectory(new File("../localhost/"));
        FileUtils.deleteDirectory(new File("localhost"));
    }

    @Test (enabled = false)
    public void testFeedImportHSql() throws Exception {
        Assert.assertEquals(4, HsqldbTestUtils.getNumberOfRows());
    }

    @Test (enabled = false)
    public void testSqoopImport() throws Exception {
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
    }

    @Test (enabled = false)
    public void testSqoopImportDeleteDatasource() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);
        LOG.info("entity -submit -type cluster -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        // Make a new datasource name into the overlay so that DATASOURCE_TEMPLATE1 and FEED_TEMPLATE3
        // are populated  with the same datasource name
        String dsName = "datasource-test-delete";
        overlay.put(DATASOURCE_NAME_KEY, dsName);
        filePath = TestContext.overlayParametersOverTemplate(TestContext.DATASOURCE_TEMPLATE1, overlay);
        LOG.info("entity -submit -type datasource -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type datasource -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE3, overlay);
        LOG.info("Submit FEED entity with datasource {} via entity -submit -type feed -file {}", dsName, filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type feed -file " + filePath));

        LOG.info("Delete datasource in-use via entity -delete -type datasource -name {}", dsName);
        Assert.assertEquals(-1, TestContext.executeWithURL("entity -delete -type datasource -name " + dsName));
    }

    @Test (enabled = false)
    public void testSqoopImport2() throws Exception {
        // create a TestContext and a test embedded cluster
        TestContext context = new TestContext();
        context.setCluster("test");
        EmbeddedCluster cluster = context.getCluster();
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);
        Map<String, String> overlay = context.getUniqueOverlay();

        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);
        LOG.info("entity -submit -type cluster -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        // Make a new datasource name into the overlay for use in DATASOURCE_TEMPLATE1 and FEED_TEMPLATE3
        String dsName = "datasource-test-2";
        overlay.put(DATASOURCE_NAME_KEY, dsName);

        // create a password file on hdfs in the following location
        String hdfsPasswordFile = "/falcon/passwordfile";
        FSDataOutputStream fos = fs.create(new Path(hdfsPasswordFile));
        IOUtils.write("sqoop", fos);
        IOUtils.closeQuietly(fos);

        // put the fully qualified HDFS password file path into overlay for substitution
        String qualifiedHdfsPath = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY) + hdfsPasswordFile;
        LOG.info("Qualifed HDFS filepath set in the overlay {}", qualifiedHdfsPath);
        overlay.put("passwordfile", qualifiedHdfsPath);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.DATASOURCE_TEMPLATE2, overlay);
        LOG.info("Submit datasource entity {} via entity -submit -type datasource -file {}", dsName, filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type datasource -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE3, overlay);
        LOG.info("Submit import FEED entity with datasource {} via entity -submit -type feed -file {}",
            dsName, filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type feed -file " + filePath));
    }

    @Test (enabled = false)
    public void testSqoopImport3() throws Exception {
        // create a TestContext and a test embedded cluster
        TestContext context = new TestContext();
        context.setCluster("test");
        EmbeddedCluster cluster = context.getCluster();
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);
        Map<String, String> overlay = context.getUniqueOverlay();

        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);
        LOG.info("entity -submit -type cluster -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        // Make a new datasource name into the overlay for use in DATASOURCE_TEMPLATE1 and FEED_TEMPLATE3
        String dsName = "datasource-test-3";
        overlay.put(DATASOURCE_NAME_KEY, dsName);

        // read the jceks provider file from resources and copy to hdfs provider path
        InputStream is = this.getClass().getResourceAsStream("/credential_provider.jceks");
        LOG.info("Opened credential_provider.jceks file from resource {}", (is == null) ? false : true);

        // create a jceks provider path on hdfs in the following location
        String hdfsProviderPath = "/falcon/providerpath";
        FSDataOutputStream fos = fs.create(new Path(hdfsProviderPath));
        LOG.info("Opened embedded cluster hdfs file for writing jceks {}", (fos == null) ? false : true);
        int numBytes = IOUtils.copy(is, fos);
        LOG.info("Copied {} bytes to hdfs provider file from resource.", numBytes);
        IOUtils.closeQuietly(is);
        IOUtils.closeQuietly(fos);

        // put the fully qualified HDFS password file path into overlay for substitution
        String qualifiedHdfsPath = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY) + hdfsProviderPath;
        LOG.info("Qualifed HDFS provider path set in the overlay {}", qualifiedHdfsPath);
        overlay.put("providerpath", qualifiedHdfsPath);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.DATASOURCE_TEMPLATE3, overlay);
        LOG.info("Submit datasource entity {} via entity -submit -type datasource -file {}", dsName, filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type datasource -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE3, overlay);
        LOG.info("Submit import FEED entity with datasource {} via entity -submit -type feed -file {}",
            dsName, filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type feed -file " + filePath));
    }

    @Test (enabled = false)
    public void testSqoopImportUsingDefaultCredential() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);
        LOG.info("entity -submit -type cluster -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        // Make a new datasource name into the overlay so that DATASOURCE_TEMPLATE4 and FEED_TEMPLATE3
        // are populated  with the same datasource name
        String dsName = "datasource-test-4";
        overlay.put(DATASOURCE_NAME_KEY, dsName);
        filePath = TestContext.overlayParametersOverTemplate(TestContext.DATASOURCE_TEMPLATE4, overlay);
        LOG.info("Submit datatsource entity {} via entity -submit -type datasource -file {}", dsName, filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type datasource -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE3, overlay);
        LOG.info("Submit import feed with datasource {} via entity -submitAndSchedule -type feed -file {}", dsName,
            filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));
    }

    @Test (enabled = false)
    public void testSqoopHCatImport() throws Exception {
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

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE5, overlay);
        LOG.info("Submit import feed with datasource {} via entity -submitAndSchedule -type feed -file {}", dsName,
            filePath);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));
    }

}
