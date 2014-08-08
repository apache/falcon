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
package org.apache.falcon.oozie.feed;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.falcon.oozie.OozieEntityBuilder;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.bundle.BUNDLEAPP;
import org.apache.falcon.oozie.bundle.COORDINATOR;
import org.apache.falcon.oozie.coordinator.CONFIGURATION.Property;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.process.AbstractTestBase;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.JAVA;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Tests for Oozie workflow definition for feed replication & retention.
 */
public class OozieFeedWorkflowBuilderTest extends AbstractTestBase {
    private EmbeddedCluster srcMiniDFS;
    private EmbeddedCluster trgMiniDFS;
    private final ConfigurationStore store = ConfigurationStore.get();
    private Cluster srcCluster;
    private Cluster trgCluster;
    private Cluster alphaTrgCluster;
    private Cluster betaTrgCluster;
    private Feed feed;
    private Feed tableFeed;
    private Feed fsReplFeed;

    private static final String SRC_CLUSTER_PATH = "/feed/src-cluster.xml";
    private static final String TRG_CLUSTER_PATH = "/feed/trg-cluster.xml";
    private static final String FEED = "/feed/feed.xml";
    private static final String TABLE_FEED = "/feed/table-replication-feed.xml";
    private static final String FS_REPLICATION_FEED = "/feed/fs-replication-feed.xml";

    @BeforeClass
    public void setUpDFS() throws Exception {
        CurrentUser.authenticate("falcon");

        srcMiniDFS = EmbeddedCluster.newCluster("cluster1");
        String srcHdfsUrl = srcMiniDFS.getConf().get("fs.default.name");

        trgMiniDFS = EmbeddedCluster.newCluster("cluster2");
        String trgHdfsUrl = trgMiniDFS.getConf().get("fs.default.name");

        cleanupStore();

        org.apache.falcon.entity.v0.cluster.Property property =
                new org.apache.falcon.entity.v0.cluster.Property();
        property.setName(OozieOrchestrationWorkflowBuilder.METASTORE_KERBEROS_PRINCIPAL);
        property.setValue("hive/_HOST");

        srcCluster = (Cluster) storeEntity(EntityType.CLUSTER, SRC_CLUSTER_PATH, srcHdfsUrl);
        srcCluster.getProperties().getProperties().add(property);

        trgCluster = (Cluster) storeEntity(EntityType.CLUSTER, TRG_CLUSTER_PATH, trgHdfsUrl);
        trgCluster.getProperties().getProperties().add(property);

        alphaTrgCluster = (Cluster) storeEntity(EntityType.CLUSTER, "/feed/trg-cluster-alpha.xml", trgHdfsUrl);
        betaTrgCluster = (Cluster) storeEntity(EntityType.CLUSTER, "/feed/trg-cluster-beta.xml", trgHdfsUrl);

        feed = (Feed) storeEntity(EntityType.FEED, FEED);
        fsReplFeed = (Feed) storeEntity(EntityType.FEED, FS_REPLICATION_FEED);
        tableFeed = (Feed) storeEntity(EntityType.FEED, TABLE_FEED);
    }

    private Entity storeEntity(EntityType type, String resource) throws Exception {
        return storeEntity(type, null, resource, null);
    }

    private Entity storeEntity(EntityType type, String resource, String writeUrl) throws Exception {
        return storeEntity(type, null, resource, writeUrl);
    }

    protected void cleanupStore() throws FalconException {
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for (String entity : entities) {
                store.remove(type, entity);
            }
        }
    }

    @AfterClass
    public void stopDFS() {
        srcMiniDFS.shutdown();
        trgMiniDFS.shutdown();
    }

    @Test
    public void testReplicationCoordsForFSStorage() throws Exception {
        OozieEntityBuilder builder = OozieEntityBuilder.get(feed);
        Path bundlePath = new Path("/projects/falcon/");
        builder.build(trgCluster, bundlePath);
        BUNDLEAPP bundle = getBundle(trgMiniDFS.getFileSystem(), bundlePath);
        List<COORDINATOR> coords = bundle.getCoordinator();

        //Assert retention coord
        COORDINATORAPP coord = getCoordinator(trgMiniDFS, coords.get(0).getAppPath());
        assertLibExtensions(coord, "retention");

        //Assert replication coord
        coord = getCoordinator(trgMiniDFS, coords.get(1).getAppPath());
        Assert.assertEquals("2010-01-01T00:40Z", coord.getStart());
        Assert.assertEquals(getWorkflowAppPath(), coord.getAction().getWorkflow().getAppPath());
        Assert.assertEquals("FALCON_FEED_REPLICATION_" + feed.getName() + "_"
                + srcCluster.getName(), coord.getName());
        Assert.assertEquals("${coord:minutes(20)}", coord.getFrequency());
        SYNCDATASET inputDataset = (SYNCDATASET) coord.getDatasets()
                .getDatasetOrAsyncDataset().get(0);
        SYNCDATASET outputDataset = (SYNCDATASET) coord.getDatasets()
                .getDatasetOrAsyncDataset().get(1);

        Assert.assertEquals("${coord:minutes(20)}", inputDataset.getFrequency());
        Assert.assertEquals("input-dataset", inputDataset.getName());
        Assert.assertEquals(
                ClusterHelper.getReadOnlyStorageUrl(srcCluster)
                        + "/examples/input-data/rawLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
                inputDataset.getUriTemplate());

        Assert.assertEquals("${coord:minutes(20)}",
                outputDataset.getFrequency());
        Assert.assertEquals("output-dataset", outputDataset.getName());
        Assert.assertEquals(ClusterHelper.getStorageUrl(trgCluster)
                + "/examples/input-data/rawLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
                        outputDataset.getUriTemplate());
        String inEventName =coord.getInputEvents().getDataIn().get(0).getName();
        String inEventDataset =coord.getInputEvents().getDataIn().get(0).getDataset();
        String inEventInstance = coord.getInputEvents().getDataIn().get(0).getInstance().get(0);
        Assert.assertEquals("input", inEventName);
        Assert.assertEquals("input-dataset", inEventDataset);
        Assert.assertEquals("${now(0,-40)}", inEventInstance);

        String outEventInstance = coord.getOutputEvents().getDataOut().get(0).getInstance();
        Assert.assertEquals("${now(0,-40)}", outEventInstance);

        HashMap<String, String> props = new HashMap<String, String>();
        for (Property prop : coord.getAction().getWorkflow().getConfiguration().getProperty()) {
            props.put(prop.getName(), prop.getValue());
        }

        // verify the replication param that feed replicator depends on
        String pathsWithPartitions = getPathsWithPartitions(srcCluster, trgCluster, feed);
        Assert.assertEquals(props.get("sourceRelativePaths"), pathsWithPartitions);

        Assert.assertEquals(props.get("sourceRelativePaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("distcpSourcePaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("distcpTargetPaths"), "${coord:dataOut('output')}");
        Assert.assertEquals(props.get("falconFeedStorageType"), Storage.TYPE.FILESYSTEM.name());

        // verify the late data params
        Assert.assertEquals(props.get("falconInputFeeds"), feed.getName());
        Assert.assertEquals(props.get("falconInPaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("falconInPaths"), pathsWithPartitions);
        Assert.assertEquals(props.get("falconInputFeedStorageTypes"), Storage.TYPE.FILESYSTEM.name());
        Assert.assertEquals(props.get("logDir"), getLogPath(trgCluster, feed));

        // verify the post processing params
        Assert.assertEquals(props.get("feedNames"), feed.getName());
        Assert.assertEquals(props.get("feedInstancePaths"), "${coord:dataOut('output')}");

        // verify workflow params
        Assert.assertEquals(props.get("userWorkflowName"), "replication-policy");
        Assert.assertEquals(props.get("userWorkflowVersion"), "0.6");
        Assert.assertEquals(props.get("userWorkflowEngine"), "falcon");

        // verify default params
        Assert.assertEquals(props.get("queueName"), "default");
        Assert.assertEquals(props.get("jobPriority"), "NORMAL");
        Assert.assertEquals(props.get("maxMaps"), "5");
        Assert.assertEquals(props.get("mapBandwidthKB"), "102400");

        assertLibExtensions(coord, "replication");
        WORKFLOWAPP wf = getWorkflowapp(trgMiniDFS.getFileSystem(), coord);
        assertWorkflowRetries(wf);

        Assert.assertFalse(Storage.TYPE.TABLE == FeedHelper.getStorageType(feed, trgCluster));
    }

    private void assertLibExtensions(COORDINATORAPP coord, String lifecycle) throws Exception {
        assertLibExtensions(trgMiniDFS.getFileSystem(), coord, EntityType.FEED, lifecycle);
    }

    private COORDINATORAPP getCoordinator(EmbeddedCluster cluster, String appPath) throws Exception {
        return getCoordinator(cluster.getFileSystem(), new Path(StringUtils.removeStart(appPath, "${nameNode}")));
    }

    private String getWorkflowAppPath() {
        return "${nameNode}/projects/falcon/REPLICATION/" + srcCluster.getName();
    }

    private void assertWorkflowRetries(COORDINATORAPP coord) throws JAXBException, IOException {
        assertWorkflowRetries(getWorkflowapp(trgMiniDFS.getFileSystem(), coord));
    }

    private void assertWorkflowRetries(WORKFLOWAPP wf) throws JAXBException, IOException {
        List<Object> actions = wf.getDecisionOrForkOrJoin();
        for (Object obj : actions) {
            if (!(obj instanceof ACTION)) {
                continue;
            }
            ACTION action = (ACTION) obj;
            String actionName = action.getName();
            if (OozieOrchestrationWorkflowBuilder.FALCON_ACTIONS.contains(actionName)) {
                Assert.assertEquals(action.getRetryMax(), "3");
                Assert.assertEquals(action.getRetryInterval(), "1");
            }
        }
    }

    @Test
    public void testReplicationCoordsForFSStorageWithMultipleTargets() throws Exception {
        OozieCoordinatorBuilder builder = OozieCoordinatorBuilder.get(fsReplFeed, Tag.REPLICATION);

        List<Properties> alphaCoords = builder.buildCoords(alphaTrgCluster, new Path("/alpha/falcon/"));
        final COORDINATORAPP alphaCoord = getCoordinator(trgMiniDFS,
            alphaCoords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));
        Assert.assertEquals(alphaCoord.getStart(), "2012-10-01T12:05Z");
        Assert.assertEquals(alphaCoord.getEnd(), "2012-10-01T12:11Z");

        String pathsWithPartitions = getPathsWithPartitions(srcCluster, alphaTrgCluster, fsReplFeed);
        assertReplCoord(alphaCoord, fsReplFeed, alphaTrgCluster.getName(), pathsWithPartitions);

        List<Properties> betaCoords = builder.buildCoords(betaTrgCluster, new Path("/beta/falcon/"));
        final COORDINATORAPP betaCoord = getCoordinator(trgMiniDFS,
            betaCoords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));
        Assert.assertEquals(betaCoord.getStart(), "2012-10-01T12:10Z");
        Assert.assertEquals(betaCoord.getEnd(), "2012-10-01T12:26Z");

        pathsWithPartitions = getPathsWithPartitions(srcCluster, betaTrgCluster, fsReplFeed);
        assertReplCoord(betaCoord, fsReplFeed, betaTrgCluster.getName(), pathsWithPartitions);
    }

    private String getPathsWithPartitions(Cluster sourceCluster, Cluster targetCluster,
                                          Feed aFeed) throws FalconException {
        String srcPart = FeedHelper.normalizePartitionExpression(
                FeedHelper.getCluster(aFeed, sourceCluster.getName()).getPartition());
        srcPart = FeedHelper.evaluateClusterExp(sourceCluster, srcPart);
        String targetPart = FeedHelper.normalizePartitionExpression(
                FeedHelper.getCluster(aFeed, targetCluster.getName()).getPartition());
        targetPart = FeedHelper.evaluateClusterExp(targetCluster, targetPart);

        String pathsWithPartitions = "${coord:dataIn('input')}/"
                + FeedHelper.normalizePartitionExpression(srcPart, targetPart);
        String parts = pathsWithPartitions.replaceAll("//+", "/");
        parts = StringUtils.stripEnd(parts, "/");
        return parts;
    }

    private void assertReplCoord(COORDINATORAPP coord, Feed aFeed, String clusterName,
                                 String pathsWithPartitions) throws JAXBException, IOException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(aFeed, clusterName);
        Date startDate = feedCluster.getValidity().getStart();
        Assert.assertEquals(coord.getStart(), SchemaHelper.formatDateUTC(startDate));

        Date endDate = feedCluster.getValidity().getEnd();
        Assert.assertEquals(coord.getEnd(), SchemaHelper.formatDateUTC(endDate));

        WORKFLOWAPP workflow = getWorkflowapp(trgMiniDFS.getFileSystem(), coord);
        assertWorkflowDefinition(fsReplFeed, workflow, false);

        ACTION replicationActionNode = getAction(workflow, "replication");
        JAVA replication = replicationActionNode.getJava();
        List<String> args = replication.getArg();
        Assert.assertEquals(args.size(), 13);

        HashMap<String, String> props = new HashMap<String, String>();
        for (Property prop : coord.getAction().getWorkflow().getConfiguration().getProperty()) {
            props.put(prop.getName(), prop.getValue());
        }

        Assert.assertEquals(props.get("sourceRelativePaths"), pathsWithPartitions);
        Assert.assertEquals(props.get("sourceRelativePaths"), "${coord:dataIn('input')}/" + srcCluster.getColo());
        Assert.assertEquals(props.get("distcpSourcePaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("distcpTargetPaths"), "${coord:dataOut('output')}");
        Assert.assertEquals(props.get("falconFeedStorageType"), Storage.TYPE.FILESYSTEM.name());
        Assert.assertEquals(props.get("maxMaps"), "33");
        Assert.assertEquals(props.get("mapBandwidthKB"), "2048");
        Assert.assertEquals(props.get("logDir"), getLogPath(trgCluster, aFeed));
    }

    public void assertWorkflowDefinition(Feed aFeed, WORKFLOWAPP workflow, boolean isTable) {
        Assert.assertEquals(EntityUtil.getWorkflowName(Tag.REPLICATION, aFeed).toString(), workflow.getName());

        boolean preProcess = RuntimeProperties.get().getProperty("feed.late.allowed", "true").equalsIgnoreCase("true");
        if (preProcess) {
            assertAction(workflow, "pre-processing", true);
        }
        assertAction(workflow, "replication", false);
        assertAction(workflow, "succeeded-post-processing", true);
        assertAction(workflow, "failed-post-processing", true);

        if (isTable) {
            assertAction(workflow, "table-import", false);
            assertAction(workflow, "table-export", false);
        }
    }

    @DataProvider(name = "secureOptions")
    private Object[][] createOptions() {
        return new Object[][] {
            {"simple"},
            {"kerberos"},
        };
    }

    @Test (dataProvider = "secureOptions")
    public void testReplicationCoordsForTableStorage(String secureOption) throws Exception {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, secureOption);

        OozieCoordinatorBuilder builder = OozieCoordinatorBuilder.get(tableFeed, Tag.REPLICATION);
        List<Properties> coords = builder.buildCoords(trgCluster, new Path("/projects/falcon/"));
        COORDINATORAPP coord = getCoordinator(trgMiniDFS, coords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));

        Assert.assertEquals("2010-01-01T00:40Z", coord.getStart());
        Assert.assertEquals(getWorkflowAppPath(),
                coord.getAction().getWorkflow().getAppPath());
        Assert.assertEquals("FALCON_FEED_REPLICATION_" + tableFeed.getName() + "_"
                + srcCluster.getName(), coord.getName());
        Assert.assertEquals("${coord:minutes(20)}", coord.getFrequency());

        SYNCDATASET inputDataset = (SYNCDATASET) coord.getDatasets()
                .getDatasetOrAsyncDataset().get(0);
        Assert.assertEquals("${coord:minutes(20)}", inputDataset.getFrequency());
        Assert.assertEquals("input-dataset", inputDataset.getName());

        String sourceRegistry = ClusterHelper.getInterface(srcCluster, Interfacetype.REGISTRY).getEndpoint();
        sourceRegistry = sourceRegistry.replace("thrift", "hcat");
        Assert.assertEquals(inputDataset.getUriTemplate(),
                sourceRegistry + "/source_db/source_clicks_table/ds=${YEAR}${MONTH}${DAY};region=${region}");

        SYNCDATASET outputDataset = (SYNCDATASET) coord.getDatasets()
                .getDatasetOrAsyncDataset().get(1);
        Assert.assertEquals(outputDataset.getFrequency(), "${coord:minutes(20)}");
        Assert.assertEquals("output-dataset", outputDataset.getName());

        String targetRegistry = ClusterHelper.getInterface(trgCluster, Interfacetype.REGISTRY).getEndpoint();
        targetRegistry = targetRegistry.replace("thrift", "hcat");
        Assert.assertEquals(outputDataset.getUriTemplate(),
                targetRegistry + "/target_db/target_clicks_table/ds=${YEAR}${MONTH}${DAY};region=${region}");

        String inEventName =coord.getInputEvents().getDataIn().get(0).getName();
        String inEventDataset =coord.getInputEvents().getDataIn().get(0).getDataset();
        String inEventInstance = coord.getInputEvents().getDataIn().get(0).getInstance().get(0);
        Assert.assertEquals("input", inEventName);
        Assert.assertEquals("input-dataset", inEventDataset);
        Assert.assertEquals("${now(0,-40)}", inEventInstance);

        String outEventInstance = coord.getOutputEvents().getDataOut().get(0).getInstance();
        Assert.assertEquals("${now(0,-40)}", outEventInstance);

        // assert FS staging area
        Path wfPath = new Path(coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", ""));
        final FileSystem fs = trgMiniDFS.getFileSystem();
        Assert.assertTrue(fs.exists(new Path(wfPath + "/scripts")));
        Assert.assertTrue(fs.exists(new Path(wfPath + "/scripts/falcon-table-export.hql")));
        Assert.assertTrue(fs.exists(new Path(wfPath + "/scripts/falcon-table-import.hql")));

        Assert.assertTrue(fs.exists(new Path(wfPath + "/conf")));
        Assert.assertTrue(fs.exists(new Path(wfPath + "/conf/falcon-source-hive-site.xml")));
        Assert.assertTrue(fs.exists(new Path(wfPath + "/conf/falcon-target-hive-site.xml")));

        HashMap<String, String> props = new HashMap<String, String>();
        for (Property prop : coord.getAction().getWorkflow().getConfiguration().getProperty()) {
            props.put(prop.getName(), prop.getValue());
        }

        final CatalogStorage srcStorage = (CatalogStorage) FeedHelper.createStorage(srcCluster, tableFeed);
        final CatalogStorage trgStorage = (CatalogStorage) FeedHelper.createStorage(trgCluster, tableFeed);

        // verify the replication param that feed replicator depends on
        Assert.assertEquals(props.get("sourceRelativePaths"), "IGNORE");

        Assert.assertTrue(props.containsKey("distcpSourcePaths"));
        Assert.assertEquals(props.get("distcpSourcePaths"),
                FeedHelper.getStagingPath(srcCluster, tableFeed, srcStorage, Tag.REPLICATION,
                        "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}" + "/" + trgCluster.getName()));

        Assert.assertTrue(props.containsKey("distcpTargetPaths"));
        Assert.assertEquals(props.get("distcpTargetPaths"),
                FeedHelper.getStagingPath(trgCluster, tableFeed, trgStorage, Tag.REPLICATION,
                        "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}" + "/" + trgCluster.getName()));

        Assert.assertEquals(props.get("falconFeedStorageType"), Storage.TYPE.TABLE.name());

        // verify table props
        assertTableStorageProperties(srcCluster, srcStorage, props, "falconSource");
        assertTableStorageProperties(trgCluster, trgStorage, props, "falconTarget");

        // verify the late data params
        Assert.assertEquals(props.get("falconInputFeeds"), tableFeed.getName());
        Assert.assertEquals(props.get("falconInPaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("falconInputFeedStorageTypes"), Storage.TYPE.TABLE.name());
        Assert.assertEquals(props.get("logDir"), getLogPath(trgCluster, tableFeed));

        // verify the post processing params
        Assert.assertEquals(props.get("feedNames"), tableFeed.getName());
        Assert.assertEquals(props.get("feedInstancePaths"), "${coord:dataOut('output')}");

        Assert.assertTrue(Storage.TYPE.TABLE == FeedHelper.getStorageType(tableFeed, trgCluster));
        assertReplicationHCatCredentials(getWorkflowapp(trgMiniDFS.getFileSystem(), coord), wfPath.toString());
    }

    private void assertReplicationHCatCredentials(WORKFLOWAPP wf, String wfPath) throws IOException {
        FileSystem fs = trgMiniDFS.getFileSystem();

        Path hiveConfPath = new Path(wfPath, "conf/falcon-source-hive-site.xml");
        Assert.assertTrue(fs.exists(hiveConfPath));

        hiveConfPath = new Path(wfPath, "conf/falcon-target-hive-site.xml");
        Assert.assertTrue(fs.exists(hiveConfPath));

        boolean isSecurityEnabled = SecurityUtil.isSecurityEnabled();
        if (isSecurityEnabled) {
            Assert.assertNotNull(wf.getCredentials());
            Assert.assertEquals(2, wf.getCredentials().getCredential().size());
        }

        List<Object> actions = wf.getDecisionOrForkOrJoin();
        for (Object obj : actions) {
            if (!(obj instanceof ACTION)) {
                continue;
            }
            ACTION action = (ACTION) obj;
            String actionName = action.getName();

            if (!isSecurityEnabled) {
                Assert.assertNull(action.getCred());
            }

            if ("recordsize".equals(actionName)) {
                Assert.assertEquals(action.getJava().getJobXml(), "${wf:appPath()}/conf/falcon-source-hive-site.xml");
                if (isSecurityEnabled) {
                    Assert.assertNotNull(action.getCred());
                    Assert.assertEquals(action.getCred(), "falconSourceHiveAuth");
                }
            } else if ("table-export".equals(actionName) && isSecurityEnabled) {
                Assert.assertNotNull(action.getCred());
                Assert.assertEquals(action.getCred(), "falconSourceHiveAuth");
            } else if ("table-import".equals(actionName) && isSecurityEnabled) {
                Assert.assertNotNull(action.getCred());
                Assert.assertEquals(action.getCred(), "falconTargetHiveAuth");
            }
        }
    }

    private void assertTableStorageProperties(Cluster cluster, CatalogStorage tableStorage,
                                              Map<String, String> props, String prefix) {
        Assert.assertEquals(props.get(prefix + "NameNode"), ClusterHelper.getStorageUrl(cluster));
        Assert.assertEquals(props.get(prefix + "JobTracker"), ClusterHelper.getMREndPoint(cluster));
        Assert.assertEquals(props.get(prefix + "HcatNode"), tableStorage.getCatalogUrl());

        Assert.assertEquals(props.get(prefix + "Database"), tableStorage.getDatabase());
        Assert.assertEquals(props.get(prefix + "Table"), tableStorage.getTable());
        Assert.assertEquals(props.get(prefix + "Partition"), "${coord:dataInPartitions('input', 'hive-export')}");
    }

    @Test
    public void testRetentionCoords() throws Exception {
        org.apache.falcon.entity.v0.feed.Cluster cluster = FeedHelper.getCluster(feed, srcCluster.getName());
        final Calendar instance = Calendar.getInstance();
        instance.roll(Calendar.YEAR, 1);
        cluster.getValidity().setEnd(instance.getTime());

        OozieCoordinatorBuilder builder = OozieCoordinatorBuilder.get(feed, Tag.RETENTION);
        List<Properties> coords = builder.buildCoords(srcCluster, new Path("/projects/falcon/"));
        COORDINATORAPP coord = getCoordinator(srcMiniDFS, coords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));

        Assert.assertEquals(coord.getAction().getWorkflow().getAppPath(), "${nameNode}/projects/falcon/RETENTION");
        Assert.assertEquals(coord.getName(), "FALCON_FEED_RETENTION_" + feed.getName());
        Assert.assertEquals(coord.getFrequency(), "${coord:hours(6)}");

        HashMap<String, String> props = new HashMap<String, String>();
        for (Property prop : coord.getAction().getWorkflow().getConfiguration().getProperty()) {
            props.put(prop.getName(), prop.getValue());
        }

        String feedDataPath = props.get("feedDataPath");
        String storageType = props.get("falconFeedStorageType");

        // verify the param that feed evictor depends on
        Assert.assertEquals(storageType, Storage.TYPE.FILESYSTEM.name());

        final Storage storage = FeedHelper.createStorage(cluster, feed);
        if (feedDataPath != null) {
            Assert.assertEquals(feedDataPath, storage.getUriTemplate()
                    .replaceAll(Storage.DOLLAR_EXPR_START_REGEX, Storage.QUESTION_EXPR_START_REGEX));
        }

        if (storageType != null) {
            Assert.assertEquals(storageType, storage.getType().name());
        }

        // verify the post processing params
        Assert.assertEquals(props.get("feedNames"), feed.getName());
        Assert.assertEquals(props.get("feedInstancePaths"), "IGNORE");
        Assert.assertEquals(props.get("logDir"), getLogPath(srcCluster, feed));

        assertWorkflowRetries(coord);
    }

    @Test (dataProvider = "secureOptions")
    public void testRetentionCoordsForTable(String secureOption) throws Exception {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, secureOption);

        org.apache.falcon.entity.v0.feed.Cluster cluster = FeedHelper.getCluster(tableFeed, trgCluster.getName());
        final Calendar instance = Calendar.getInstance();
        instance.roll(Calendar.YEAR, 1);
        cluster.getValidity().setEnd(instance.getTime());

        OozieCoordinatorBuilder builder = OozieCoordinatorBuilder.get(tableFeed, Tag.RETENTION);
        List<Properties> coords = builder.buildCoords(trgCluster, new Path("/projects/falcon/"));
        COORDINATORAPP coord = getCoordinator(trgMiniDFS, coords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));

        Assert.assertEquals(coord.getAction().getWorkflow().getAppPath(), "${nameNode}/projects/falcon/RETENTION");
        Assert.assertEquals(coord.getName(), "FALCON_FEED_RETENTION_" + tableFeed.getName());
        Assert.assertEquals(coord.getFrequency(), "${coord:hours(6)}");

        HashMap<String, String> props = new HashMap<String, String>();
        for (Property prop : coord.getAction().getWorkflow().getConfiguration().getProperty()) {
            props.put(prop.getName(), prop.getValue());
        }

        String feedDataPath = props.get("feedDataPath");
        String storageType = props.get("falconFeedStorageType");

        // verify the param that feed evictor depends on
        Assert.assertEquals(storageType, Storage.TYPE.TABLE.name());

        final Storage storage = FeedHelper.createStorage(cluster, tableFeed);
        if (feedDataPath != null) {
            Assert.assertEquals(feedDataPath, storage.getUriTemplate()
                    .replaceAll(Storage.DOLLAR_EXPR_START_REGEX, Storage.QUESTION_EXPR_START_REGEX));
        }

        if (storageType != null) {
            Assert.assertEquals(storageType, storage.getType().name());
        }

        // verify the post processing params
        Assert.assertEquals(props.get("feedNames"), tableFeed.getName());
        Assert.assertEquals(props.get("feedInstancePaths"), "IGNORE");
        Assert.assertEquals(props.get("logDir"), getLogPath(trgCluster, tableFeed));

        assertWorkflowRetries(coord);

        Assert.assertTrue(Storage.TYPE.TABLE == FeedHelper.getStorageType(tableFeed, trgCluster));
        assertHCatCredentials(
            getWorkflowapp(trgMiniDFS.getFileSystem(), coord),
            new Path(coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "")).toString());
    }

    private void assertHCatCredentials(WORKFLOWAPP wf, String wfPath) throws IOException {
        Path hiveConfPath = new Path(wfPath, "conf/hive-site.xml");
        FileSystem fs = trgMiniDFS.getFileSystem();
        Assert.assertTrue(fs.exists(hiveConfPath));

        if (SecurityUtil.isSecurityEnabled()) {
            Assert.assertNotNull(wf.getCredentials());
            Assert.assertEquals(1, wf.getCredentials().getCredential().size());
        }

        List<Object> actions = wf.getDecisionOrForkOrJoin();
        for (Object obj : actions) {
            if (!(obj instanceof ACTION)) {
                continue;
            }
            ACTION action = (ACTION) obj;
            String actionName = action.getName();

            if ("eviction".equals(actionName)) {
                Assert.assertEquals(action.getJava().getJobXml(), "${wf:appPath()}/conf/hive-site.xml");
                if (SecurityUtil.isSecurityEnabled()) {
                    Assert.assertNotNull(action.getCred());
                    Assert.assertEquals(action.getCred(), "falconHiveAuth");
                }
            }
        }
    }

    private String getLogPath(Cluster aCluster, Feed aFeed) {
        Path logPath = EntityUtil.getLogPath(aCluster, aFeed);
        return (logPath.toUri().getScheme() == null ? "${nameNode}" : "") + logPath;
    }
}
