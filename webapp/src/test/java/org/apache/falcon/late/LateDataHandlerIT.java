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

package org.apache.falcon.late;

import org.apache.falcon.catalog.CatalogPartition;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.catalog.HiveCatalogService;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.latedata.LateDataHandler;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.FSUtils;
import org.apache.falcon.util.HiveTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hcatalog.api.HCatClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for LateDataHandler with table storage.
 */
@Test
public class LateDataHandlerIT {

    private static final String DATABASE_NAME = "falcon_db";
    private static final String TABLE_NAME = "input_table";
    private static final String PARTITION_VALUE = "2012-04-21-00"; // ${YEAR}-${MONTH}-${DAY}-${HOUR}
    private static final String LATE_DATA_DIR = "/falcon/test/late/foo/logs/latedata/2013-09-24-00-00/primary-cluster";

    private final TestContext context = new TestContext();
    private String storageUrl;
    private String metastoreUrl;
    private FileSystem fs;

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.cleanupStore();

        String filePath = TestContext.overlayParametersOverTemplate(
                TestContext.CLUSTER_TEMPLATE, context.getUniqueOverlay());
        context.setCluster(filePath);

        Cluster cluster = context.getCluster().getCluster();
        fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
        storageUrl = ClusterHelper.getStorageUrl(cluster);
        metastoreUrl = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY).getEndpoint();

        copyDataAndScriptsToHDFS();

        setupHiveMetastore();
    }

    private void copyDataAndScriptsToHDFS() throws IOException {
        // copyTestDataToHDFS
        FSUtils.copyResourceToHDFS(
                "/apps/data/data.txt", "data.txt", storageUrl + "/falcon/test/input/" + PARTITION_VALUE);
    }

    private void setupHiveMetastore() throws Exception {
        HiveTestUtils.createDatabase(metastoreUrl, DATABASE_NAME);
        final List<String> partitionKeys = Arrays.asList("ds");
        HiveTestUtils.createTable(metastoreUrl, DATABASE_NAME, TABLE_NAME, partitionKeys);
        final String sourcePath = storageUrl + "/falcon/test/input/" + PARTITION_VALUE;
        HiveTestUtils.loadData(metastoreUrl, DATABASE_NAME, TABLE_NAME, sourcePath, PARTITION_VALUE);
    }

    @AfterClass
    public void tearDown() throws Exception {
        HiveTestUtils.dropTable(metastoreUrl, DATABASE_NAME, TABLE_NAME);
        HiveTestUtils.dropDatabase(metastoreUrl, DATABASE_NAME);

        cleanupFS();
    }

    private void cleanupFS() throws IOException {
        fs.delete(new Path("/falcon/test/input/" + PARTITION_VALUE), true);
        fs.delete(new Path("/apps/data"), true);
        fs.delete(new Path(LATE_DATA_DIR), true);
    }

    @Test
    public void testLateDataHandlerForTableStorage() throws Exception {
        String lateDataDir = storageUrl + LATE_DATA_DIR;
        String feedUriTemplate = metastoreUrl.replace("thrift", "hcat") + "/"
                + DATABASE_NAME + "/" + TABLE_NAME + "/ds=" + PARTITION_VALUE;

        String[] args = {
            "-out", lateDataDir,
            "-paths", feedUriTemplate,
            "-falconInputNames", "foo",
            "-falconInputFeedStorageTypes", "TABLE",
        };


        LateDataHandler.main(args);

        Path lateDataPath = new Path(storageUrl + LATE_DATA_DIR);
        Assert.assertTrue(fs.exists(lateDataPath));

        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(lateDataPath)));
        String line;
        Map<String, Long> recordedMetrics = new LinkedHashMap<String, Long>();
        while ((line = in.readLine()) != null) {
            if (line.isEmpty()) {
                continue;
            }
            int index = line.indexOf('=');
            String key = line.substring(0, index);
            long size = Long.parseLong(line.substring(index + 1));
            recordedMetrics.put(key, size);
        }

        LateDataHandler lateDataHandler = new LateDataHandler();
        final long metric = lateDataHandler.computeStorageMetric(args[3], args[7], new Configuration());
        Assert.assertEquals(recordedMetrics.get("foo").longValue(), metric);

        final String changes = lateDataHandler.detectChanges(lateDataPath, recordedMetrics, new Configuration());
        Assert.assertEquals("", changes);
    }

    @Test
    public void testLateDataHandlerForTableStorageWithLate() throws Exception {
        String lateDataDir = storageUrl + LATE_DATA_DIR;
        String feedUriTemplate = metastoreUrl.replace("thrift", "hcat") + "/"
                + DATABASE_NAME + "/" + TABLE_NAME + "/ds=" + PARTITION_VALUE;

        String[] args = {
            "-out", lateDataDir,
            "-paths", feedUriTemplate,
            "-falconInputNames", "foo",
            "-falconInputFeedStorageTypes", "TABLE",
        };

        LateDataHandler.main(args);

        Path lateDataPath = new Path(storageUrl + LATE_DATA_DIR);
        Assert.assertTrue(fs.exists(lateDataPath));

        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(lateDataPath)));
        String line;
        Map<String, Long> recordedMetrics = new LinkedHashMap<String, Long>();
        while ((line = in.readLine()) != null) {
            if (line.isEmpty()) {
                continue;
            }
            int index = line.indexOf('=');
            String key = line.substring(0, index);
            long size = Long.parseLong(line.substring(index + 1));
            recordedMetrics.put(key, size);
        }

        reinstatePartition();

        LateDataHandler lateDataHandler = new LateDataHandler();
        long metric = lateDataHandler.computeStorageMetric(args[3], args[7], new Configuration());
        Assert.assertFalse(recordedMetrics.get("foo") == metric);

        Map<String, Long> computedMetrics = new LinkedHashMap<String, Long>();
        computedMetrics.put("foo", metric);

        String changes = lateDataHandler.detectChanges(lateDataPath, computedMetrics, new Configuration());
        Assert.assertEquals("foo", changes);
    }

    private void reinstatePartition() throws Exception {
        final HCatClient client = HiveCatalogService.get(metastoreUrl);

        Map<String, String> partitionSpec = new HashMap<String, String>();
        partitionSpec.put("ds", PARTITION_VALUE);

        client.dropPartitions(DATABASE_NAME, TABLE_NAME, partitionSpec, true);

        Thread.sleep(1000); // sleep so the next add is delayed a bit

        HCatAddPartitionDesc reinstatedPartition = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, partitionSpec).build();
        client.addPartition(reinstatedPartition);

        CatalogPartition reInstatedPartition = CatalogServiceFactory.getCatalogService().getPartition(
                metastoreUrl, DATABASE_NAME, TABLE_NAME, partitionSpec);
        Assert.assertNotNull(reInstatedPartition);
    }
}
