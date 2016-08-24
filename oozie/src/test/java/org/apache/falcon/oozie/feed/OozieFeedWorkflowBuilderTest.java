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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
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
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.falcon.oozie.OozieEntityBuilder;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.bundle.BUNDLEAPP;
import org.apache.falcon.oozie.bundle.COORDINATOR;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.process.AbstractTestBase;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CONFIGURATION;
import org.apache.falcon.oozie.workflow.JAVA;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.service.LifecyclePolicyMap;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
import java.util.Iterator;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
    private Feed lifecycleRetentionFeed;
    private Feed lifecycleLocalRetentionFeed;
    private Feed fsReplFeedCounter;

    private static final String SRC_CLUSTER_PATH = "/feed/src-cluster.xml";
    private static final String TRG_CLUSTER_PATH = "/feed/trg-cluster.xml";
    private static final String FEED = "/feed/feed.xml";
    private static final String TABLE_FEED = "/feed/table-replication-feed.xml";
    private static final String FS_REPLICATION_FEED = "/feed/fs-replication-feed.xml";
    private static final String FS_RETENTION_LIFECYCLE_FEED = "/feed/fs-retention-lifecycle-feed.xml";
    private static final String FS_LOCAL_RETENTION_LIFECYCLE_FEED = "/feed/fs-local-retention-lifecycle-feed.xml";
    private static final String FS_REPLICATION_FEED_COUNTER = "/feed/fs-replication-feed-counters.xml";

    @BeforeClass
    public void setUpDFS() throws Exception {
        CurrentUser.authenticate(System.getProperty("user.name"));

        srcMiniDFS = EmbeddedCluster.newCluster("cluster1");
        String srcHdfsUrl = srcMiniDFS.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);

        trgMiniDFS = EmbeddedCluster.newCluster("cluster2");
        String trgHdfsUrl = trgMiniDFS.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);

        LifecyclePolicyMap.get().init();
        cleanupStore();

        org.apache.falcon.entity.v0.cluster.Property property =
                new org.apache.falcon.entity.v0.cluster.Property();
        property.setName(SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
        property.setValue("hive/_HOST");

        srcCluster = (Cluster) storeEntity(EntityType.CLUSTER, SRC_CLUSTER_PATH, srcHdfsUrl);
        srcCluster.getProperties().getProperties().add(property);

        trgCluster = (Cluster) storeEntity(EntityType.CLUSTER, TRG_CLUSTER_PATH, trgHdfsUrl);
        trgCluster.getProperties().getProperties().add(property);

        alphaTrgCluster = (Cluster) storeEntity(EntityType.CLUSTER, "/feed/trg-cluster-alpha.xml", trgHdfsUrl);
        betaTrgCluster = (Cluster) storeEntity(EntityType.CLUSTER, "/feed/trg-cluster-beta.xml", trgHdfsUrl);

        feed = (Feed) storeEntity(EntityType.FEED, FEED);
        fsReplFeed = (Feed) storeEntity(EntityType.FEED, FS_REPLICATION_FEED);
        fsReplFeedCounter = (Feed) storeEntity(EntityType.FEED, FS_REPLICATION_FEED_COUNTER);
        tableFeed = (Feed) storeEntity(EntityType.FEED, TABLE_FEED);
        lifecycleRetentionFeed = (Feed) storeEntity(EntityType.FEED, FS_RETENTION_LIFECYCLE_FEED);
        lifecycleLocalRetentionFeed = (Feed) storeEntity(EntityType.FEED, FS_LOCAL_RETENTION_LIFECYCLE_FEED);
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

    @DataProvider(name = "keepInstancesPostValidity")
    private Object[][] keepInstancesPostValidity() {
        return new Object[][] {
            {"false", "2099-01-01T02:00Z"},
            {"true", "2099-01-01T00:00Z"},
        };
    }

    @Test(dataProvider = "keepInstancesPostValidity")
    public void testRetentionWithLifecycle(String keepInstancesPostValidity, String endTime) throws Exception {
        RuntimeProperties.get().setProperty("falcon.retention.keep.instances.beyond.validity",
                keepInstancesPostValidity);
        OozieEntityBuilder builder = OozieEntityBuilder.get(lifecycleRetentionFeed);
        Path bundlePath = new Path("/projects/falcon/");
        builder.build(trgCluster, bundlePath);

        BUNDLEAPP bundle = getBundle(trgMiniDFS.getFileSystem(), bundlePath);
        List<COORDINATOR> coords = bundle.getCoordinator();
        COORDINATORAPP coord = getCoordinator(trgMiniDFS, coords.get(0).getAppPath());
        assertLibExtensions(coord, "retention");
        HashMap<String, String> props = getCoordProperties(coord);
        Assert.assertEquals(props.get("ENTITY_PATH"), bundlePath.toString() + "/RETENTION");
        Assert.assertEquals(props.get("queueName"), "ageBasedDeleteQueue");
        Assert.assertEquals(coord.getFrequency(), "${coord:hours(17)}");
        Assert.assertEquals(coord.getEnd(), endTime);
        Assert.assertEquals(coord.getTimezone(), "UTC");

        HashMap<String, String> wfProps = getWorkflowProperties(trgMiniDFS.getFileSystem(), coord);
        Assert.assertEquals(wfProps.get("feedNames"), lifecycleRetentionFeed.getName());
        Assert.assertTrue(StringUtils.equals(wfProps.get("entityType"), EntityType.FEED.name()));
        Assert.assertEquals(wfProps.get("userWorkflowEngine"), "falcon");
        Assert.assertEquals(wfProps.get("queueName"), "ageBasedDeleteQueue");
        Assert.assertEquals(wfProps.get("limit"), "hours(2)");
        Assert.assertEquals(wfProps.get("jobPriority"), "LOW");
    }

    @Test
    public void testLocalOnlyRetentionLifecycle() throws Exception {
        OozieEntityBuilder builder = OozieEntityBuilder.get(lifecycleLocalRetentionFeed);
        Path bundlePath = new Path("/projects/falcon/");
        builder.build(trgCluster, bundlePath);

        BUNDLEAPP bundle = getBundle(trgMiniDFS.getFileSystem(), bundlePath);
        List<COORDINATOR> coords = bundle.getCoordinator();
        COORDINATORAPP coord = getCoordinator(trgMiniDFS, coords.get(0).getAppPath());
        assertLibExtensions(coord, "retention");
        HashMap<String, String> props = getCoordProperties(coord);
        Assert.assertEquals(props.get("ENTITY_PATH"), bundlePath.toString() + "/RETENTION");
        Assert.assertEquals(coord.getFrequency(), "${coord:hours(12)}");
        Assert.assertEquals(coord.getTimezone(), "UTC");

        HashMap<String, String> wfProps = getWorkflowProperties(trgMiniDFS.getFileSystem(), coord);
        Assert.assertEquals(wfProps.get("feedNames"), lifecycleLocalRetentionFeed.getName());
        Assert.assertTrue(StringUtils.equals(wfProps.get("entityType"), EntityType.FEED.name()));
        Assert.assertEquals(wfProps.get("userWorkflowEngine"), "falcon");
        Assert.assertEquals(wfProps.get("queueName"), "local");
        Assert.assertEquals(wfProps.get("limit"), "hours(4)");
        Assert.assertEquals(wfProps.get("jobPriority"), "HIGH");
    }

    @Test
    public void testRetentionFrequency() throws Exception {
        feed.setFrequency(new Frequency("minutes(36000)"));
        buildCoordAndValidateFrequency("${coord:days(1)}");

        feed.setFrequency(new Frequency("hours(2)"));
        buildCoordAndValidateFrequency("${coord:hours(6)}");

        feed.setFrequency(new Frequency("minutes(50)"));
        buildCoordAndValidateFrequency("${coord:hours(6)}");

        feed.setFrequency(new Frequency("days(1)"));
        buildCoordAndValidateFrequency("${coord:days(1)}");
    }

    private void buildCoordAndValidateFrequency(final String expectedFrequency) throws Exception {
        // retention on src cluster
        OozieCoordinatorBuilder builder = OozieCoordinatorBuilder.get(feed, Tag.RETENTION);
        List<Properties> srcCoords = builder.buildCoords(
                srcCluster, new Path("/projects/falcon/"));
        COORDINATORAPP srcCoord = getCoordinator(srcMiniDFS, srcCoords.get(0).getProperty(OozieEntityBuilder
                .ENTITY_PATH));

        // Assert src coord frequency
        Assert.assertEquals(srcCoord.getFrequency(), expectedFrequency);

        // retention on target cluster
        OozieEntityBuilder entityBuilder = OozieEntityBuilder.get(feed);
        Path bundlePath = new Path("/projects/falcon/");
        entityBuilder.build(trgCluster, bundlePath);
        BUNDLEAPP bundle = getBundle(trgMiniDFS.getFileSystem(), bundlePath);
        List<COORDINATOR> coords = bundle.getCoordinator();

        COORDINATORAPP tgtCoord = getCoordinator(trgMiniDFS, coords.get(0).getAppPath());
        // Assert target coord frequency
        Assert.assertEquals(tgtCoord.getFrequency(), expectedFrequency);
    }

    @Test
    public void testPostProcessing() throws Exception{
        StartupProperties.get().setProperty("falcon.postprocessing.enable", "false");
        OozieEntityBuilder builder = OozieEntityBuilder.get(fsReplFeed);
        Path bundlePath = new Path("/projects/falcon/");
        builder.build(alphaTrgCluster, bundlePath);
        BUNDLEAPP bundle = getBundle(trgMiniDFS.getFileSystem(), bundlePath);
        List<COORDINATOR> coords = bundle.getCoordinator();

        Boolean foundUserAction = false;
        Boolean foundPostProcessing = false;
        Iterator<COORDINATOR> coordIterator = coords.iterator();

        while(coordIterator.hasNext()){
            COORDINATORAPP coord = getCoordinator(trgMiniDFS, coordIterator.next().getAppPath());
            WORKFLOWAPP workflow = getWorkflowapp(trgMiniDFS.getFileSystem(), coord);
            Iterator<Object> workflowIterator = workflow.getDecisionOrForkOrJoin().iterator();
            while (workflowIterator.hasNext()){
                Object object = workflowIterator.next();
                if (ACTION.class.isAssignableFrom(object.getClass())){
                    ACTION action = (ACTION) object;
                    if (action.getName().equals("eviction") || action.getName().equals("replication")){
                        foundUserAction = true;
                    }
                    if (action.getName().contains("post")){
                        foundPostProcessing = true;
                    }
                }
            }
        }

        assertTrue(foundUserAction);
        assertFalse(foundPostProcessing);
        StartupProperties.get().setProperty("falcon.postprocessing.enable", "true");
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
        Assert.assertEquals("2", coord.getControls().getConcurrency());
        Assert.assertEquals("120", coord.getControls().getTimeout());
        Assert.assertEquals("FIFO", coord.getControls().getExecution());
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

        HashMap<String, String> props = getCoordProperties(coord);

        verifyEntityProperties(trgCluster, srcCluster,
                WorkflowExecutionContext.EntityOperations.REPLICATE, props);

        HashMap<String, String> wfProps = getWorkflowProperties(trgMiniDFS.getFileSystem(), coord);
        verifyEntityProperties(feed, trgCluster, WorkflowExecutionContext.EntityOperations.REPLICATE, wfProps);
        verifyBrokerProperties(trgCluster, wfProps);

        // verify the replication param that feed replicator depends on
        String pathsWithPartitions = getPathsWithPartitions(srcCluster, trgCluster, feed);
        Assert.assertEquals(props.get("sourceRelativePaths"), pathsWithPartitions);

        Assert.assertEquals(props.get("sourceRelativePaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("distcpSourcePaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("distcpTargetPaths"), "${coord:dataOut('output')}");
        Assert.assertEquals(props.get("falconFeedStorageType"), Storage.TYPE.FILESYSTEM.name());

        // verify the late data params
        Assert.assertEquals(props.get("falconInputFeeds"), feed.getName());
        Assert.assertEquals(props.get(WorkflowExecutionArgs.INPUT_NAMES.getName()), feed.getName());
        Assert.assertEquals(props.get("falconInPaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("falconInPaths"), pathsWithPartitions);
        Assert.assertEquals(props.get("falconInputFeedStorageTypes"), Storage.TYPE.FILESYSTEM.name());

        // verify the post processing params
        Assert.assertEquals(props.get("feedNames"), feed.getName());
        Assert.assertEquals(props.get("feedInstancePaths"), "${coord:dataOut('output')}");

        // verify workflow params
        Assert.assertEquals(wfProps.get("userWorkflowName"), "replication-policy");
        Assert.assertEquals(wfProps.get("userWorkflowVersion"), "0.6");
        Assert.assertEquals(wfProps.get("userWorkflowEngine"), "falcon");

        // verify default params
        Assert.assertEquals(wfProps.get("queueName"), "default");
        Assert.assertEquals(wfProps.get("jobPriority"), "NORMAL");
        Assert.assertEquals(wfProps.get("maxMaps"), "5");
        Assert.assertEquals(wfProps.get("mapBandwidth"), "100");

        assertLibExtensions(coord, "replication");
        WORKFLOWAPP wf = getWorkflowapp(trgMiniDFS.getFileSystem(), coord);
        assertWorkflowRetries(wf);

        Assert.assertFalse(Storage.TYPE.TABLE == FeedHelper.getStorageType(feed, trgCluster));
    }

    private void assertLibExtensions(COORDINATORAPP coord, String lifecycle) throws Exception {
        assertLibExtensions(trgMiniDFS.getFileSystem(), coord, EntityType.FEED, lifecycle);
    }

    private COORDINATORAPP getCoordinator(EmbeddedCluster cluster, String appPath) throws Exception {
        return getCoordinator(cluster.getFileSystem(),
                new Path(StringUtils.removeStart(appPath, "${nameNode}")));
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

        List<Properties> alphaCoords = builder.buildCoords(alphaTrgCluster,
                new Path("/alpha/falcon/"));
        final COORDINATORAPP alphaCoord = getCoordinator(trgMiniDFS,
            alphaCoords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));
        Assert.assertEquals(alphaCoord.getStart(), "2012-10-01T12:05Z");
        Assert.assertEquals(alphaCoord.getEnd(), "2012-10-01T12:11Z");

        String pathsWithPartitions = getPathsWithPartitions(srcCluster, alphaTrgCluster, fsReplFeed);
        assertReplCoord(alphaCoord, fsReplFeed, alphaTrgCluster, pathsWithPartitions);

        List<Properties> betaCoords = builder.buildCoords(betaTrgCluster, new Path("/beta/falcon/"));
        final COORDINATORAPP betaCoord = getCoordinator(trgMiniDFS,
            betaCoords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));
        Assert.assertEquals(betaCoord.getStart(), "2012-10-01T12:10Z");
        Assert.assertEquals(betaCoord.getEnd(), "2012-10-01T12:26Z");

        pathsWithPartitions = getPathsWithPartitions(srcCluster, betaTrgCluster, fsReplFeed);
        assertReplCoord(betaCoord, fsReplFeed, betaTrgCluster, pathsWithPartitions);
    }

    @Test
    public void testReplicationWithCounters() throws Exception {
        OozieCoordinatorBuilder builder = OozieCoordinatorBuilder.get(fsReplFeedCounter, Tag.REPLICATION);
        List<Properties> alphaCoords = builder.buildCoords(alphaTrgCluster, new Path("/alpha/falcon"));
        final COORDINATORAPP alphaCoord = getCoordinator(trgMiniDFS,
                alphaCoords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));
        Assert.assertEquals(alphaCoord.getStart(), "2012-10-01T12:05Z");
        Assert.assertEquals(alphaCoord.getEnd(), "2012-10-01T12:11Z");
        String pathsWithPartitions = getPathsWithPartitions(srcCluster, alphaTrgCluster, fsReplFeedCounter);
        assertReplCoord(alphaCoord, fsReplFeedCounter, alphaTrgCluster, pathsWithPartitions);
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

    private void assertReplCoord(COORDINATORAPP coord, Feed aFeed, Cluster aCluster,
                                 String pathsWithPartitions) throws Exception {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                FeedHelper.getCluster(aFeed, aCluster.getName());
        Date startDate = feedCluster.getValidity().getStart();
        Assert.assertEquals(coord.getStart(), SchemaHelper.formatDateUTC(startDate));

        Date endDate = feedCluster.getValidity().getEnd();
        Assert.assertEquals(coord.getEnd(), SchemaHelper.formatDateUTC(endDate));

        WORKFLOWAPP workflow = getWorkflowapp(trgMiniDFS.getFileSystem(), coord);
        assertWorkflowDefinition(aFeed, workflow, false);

        ACTION replicationActionNode = getAction(workflow, "replication");
        JAVA replication = replicationActionNode.getJava();
        List<String> args = replication.getArg();
        if (args.contains("-counterLogDir")) {
            Assert.assertEquals(args.size(), 17);
        } else {
            Assert.assertEquals(args.size(), 15);
        }

        HashMap<String, String> props = getCoordProperties(coord);

        Assert.assertEquals(props.get("sourceRelativePaths"), pathsWithPartitions);
        Assert.assertEquals(props.get("sourceRelativePaths"), "${coord:dataIn('input')}/" + srcCluster.getColo());
        Assert.assertEquals(props.get("distcpSourcePaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("distcpTargetPaths"), "${coord:dataOut('output')}");
        Assert.assertEquals(props.get("falconFeedStorageType"), Storage.TYPE.FILESYSTEM.name());

        verifyEntityProperties(aCluster, srcCluster,
                WorkflowExecutionContext.EntityOperations.REPLICATE, props);

        HashMap<String, String> wfProps = getWorkflowProperties(trgMiniDFS.getFileSystem(), coord);
        verifyEntityProperties(aFeed, aCluster, WorkflowExecutionContext.EntityOperations.REPLICATE, wfProps);
        verifyBrokerProperties(aCluster, wfProps);

        Assert.assertEquals(wfProps.get("maxMaps"), "33");
        Assert.assertEquals(wfProps.get("mapBandwidth"), "2");
    }

    public void assertWorkflowDefinition(Feed aFeed, WORKFLOWAPP workflow, boolean isTable) {
        Assert.assertEquals(EntityUtil.getWorkflowName(Tag.REPLICATION, aFeed).toString(),
                workflow.getName());

        boolean preProcess = RuntimeProperties.get().getProperty("feed.late.allowed", "true").equalsIgnoreCase(
                "true");
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


        HashMap<String, String> props = getCoordProperties(coord);

        final CatalogStorage srcStorage = (CatalogStorage) FeedHelper.createStorage(srcCluster,
                tableFeed);
        final CatalogStorage trgStorage = (CatalogStorage) FeedHelper.createStorage(trgCluster,
                tableFeed);

        // verify the replication param that feed replicator depends on
        Assert.assertEquals(props.get("sourceRelativePaths"), "IGNORE");

        Assert.assertTrue(props.containsKey("distcpSourcePaths"));
        final String distcpSourcePaths = props.get("distcpSourcePaths");
        Assert.assertEquals(distcpSourcePaths,
                FeedHelper.getStagingPath(true, srcCluster, tableFeed, srcStorage, Tag.REPLICATION,
                        "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}" + "/" + trgCluster.getName()));
        Assert.assertTrue(props.containsKey("falconSourceStagingDir"));

        final String falconSourceStagingDir = props.get("falconSourceStagingDir");
        Assert.assertEquals(falconSourceStagingDir,
                FeedHelper.getStagingPath(false, srcCluster, tableFeed, srcStorage, Tag.REPLICATION,
                        "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}" + "/" + trgCluster.getName()));

        String exportPath = falconSourceStagingDir.substring(
                ClusterHelper.getStorageUrl(srcCluster).length(), falconSourceStagingDir.length());
        String distCPPath = distcpSourcePaths.substring(
                ClusterHelper.getReadOnlyStorageUrl(srcCluster).length(), distcpSourcePaths.length());
        Assert.assertEquals(exportPath, distCPPath);

        Assert.assertTrue(props.containsKey("distcpTargetPaths"));
        Assert.assertEquals(props.get("distcpTargetPaths"),
                FeedHelper.getStagingPath(false, trgCluster, tableFeed, trgStorage, Tag.REPLICATION,
                        "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}" + "/" + trgCluster.getName()));

        Assert.assertEquals(props.get("falconFeedStorageType"), Storage.TYPE.TABLE.name());

        // verify table props
        assertTableStorageProperties(srcCluster, srcStorage, props, "falconSource");
        assertTableStorageProperties(trgCluster, trgStorage, props, "falconTarget");

        // verify the late data params
        Assert.assertEquals(props.get("falconInputFeeds"), tableFeed.getName());
        Assert.assertEquals(props.get("falconInPaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get(WorkflowExecutionArgs.INPUT_NAMES.getName()), tableFeed.getName());
        Assert.assertEquals(props.get("falconInputFeedStorageTypes"), Storage.TYPE.TABLE.name());

        // verify the post processing params
        Assert.assertEquals(props.get("feedNames"), tableFeed.getName());
        Assert.assertEquals(props.get("feedInstancePaths"), "${coord:dataOut('output')}");

        Assert.assertTrue(Storage.TYPE.TABLE == FeedHelper.getStorageType(tableFeed, trgCluster));
        assertReplicationHCatCredentials(getWorkflowapp(trgMiniDFS.getFileSystem(), coord),
                wfPath.toString());

        HashMap<String, String> wfProps = getWorkflowProperties(trgMiniDFS.getFileSystem(), coord);
        verifyEntityProperties(tableFeed, trgCluster, WorkflowExecutionContext.EntityOperations.REPLICATE, wfProps);
        verifyBrokerProperties(trgCluster, wfProps);
    }

    private void assertReplicationHCatCredentials(WORKFLOWAPP wf, String wfPath) throws IOException {
        FileSystem fs = trgMiniDFS.getFileSystem();



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
            } else if ("replication".equals(actionName)) {
                List<CONFIGURATION.Property> properties =
                        action.getJava().getConfiguration().getProperty();
                for (CONFIGURATION.Property property : properties) {
                    if (property.getName().equals("mapreduce.job.hdfs-servers")) {
                        Assert.assertEquals(property.getValue(),
                                ClusterHelper.getReadOnlyStorageUrl(srcCluster)
                                        + "," + ClusterHelper.getStorageUrl(trgCluster));
                    }
                }
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

    @DataProvider(name = "uMaskOptions")
    private Object[][] createUMaskOptions() {
        return new Object[][] {
            {"000"}, // {FsAction.ALL, FsAction.ALL, FsAction.ALL},
            {"077"}, // {FsAction.ALL, FsAction.NONE, FsAction.NONE}
            {"027"}, // {FsAction.ALL, FsAction.READ_EXECUTE, FsAction.NONE}
            {"017"}, // {FsAction.ALL, FsAction.READ_WRITE, FsAction.NONE}
            {"012"}, // {FsAction.ALL, FsAction.READ_WRITE, FsAction.READ_EXECUTE}
            {"022"}, // {FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE}
        };
    }

    @Test(dataProvider = "uMaskOptions")
    public void testRetentionCoords(String umask) throws Exception {
        FileSystem fs = srcMiniDFS.getFileSystem();
        Configuration conf = fs.getConf();
        conf.set("fs.permissions.umask-mode", umask);

        OozieEntityBuilder feedBuilder = OozieEntityBuilder.get(feed);
        Path bundlePath = new Path("/projects/falcon/");
        feedBuilder.build(trgCluster, bundlePath);

        // ClusterHelper constructs new fs Conf. Add it to cluster properties so that it gets added to FS conf
        setUmaskInFsConf(srcCluster, umask);

        org.apache.falcon.entity.v0.feed.Cluster cluster = FeedHelper.getCluster(feed, srcCluster.getName());
        Calendar startCal = Calendar.getInstance();
        Calendar endCal = Calendar.getInstance();
        endCal.add(Calendar.DATE, 1);
        cluster.getValidity().setEnd(endCal.getTime());
        RuntimeProperties.get().setProperty("falcon.retention.keep.instances.beyond.validity", "false");

        OozieCoordinatorBuilder builder = OozieCoordinatorBuilder.get(feed, Tag.RETENTION);
        List<Properties> coords = builder.buildCoords(srcCluster, new Path("/projects/falcon/" + umask));
        COORDINATORAPP coord = getCoordinator(srcMiniDFS, coords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));

        Assert.assertEquals(coord.getAction().getWorkflow().getAppPath(),
                "${nameNode}/projects/falcon/" + umask + "/RETENTION");
        Assert.assertEquals(coord.getName(), "FALCON_FEED_RETENTION_" + feed.getName());
        Assert.assertEquals(coord.getFrequency(), "${coord:hours(6)}");

        Assert.assertEquals(coord.getStart(), DateUtil.getDateFormatFromTime(startCal.getTimeInMillis()));
        Date endDate = DateUtils.addSeconds(endCal.getTime(),
                FeedHelper.getRetentionLimitInSeconds(feed, srcCluster.getName()));
        Assert.assertEquals(coord.getEnd(), DateUtil.getDateFormatFromTime(endDate.getTime()));

        HashMap<String, String> props = getCoordProperties(coord);

        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);

        String feedDataPath = wfProps.get("feedDataPath");
        String storageType = wfProps.get("falconFeedStorageType");

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
        Assert.assertEquals(wfProps.get("feedNames"), feed.getName());
        Assert.assertEquals(wfProps.get("feedInstancePaths"), "IGNORE");

        assertWorkflowRetries(getWorkflowapp(srcMiniDFS.getFileSystem(), coord));

        try {
            verifyClusterLocationsUMask(srcCluster, fs);
            verifyWorkflowUMask(fs, coord, umask);
        } finally {
            cleanupWorkflowState(fs, coord);
            FileSystem.closeAll();
        }
    }

    @Test (dataProvider = "secureOptions")
    public void testRetentionCoordsForTable(String secureOption) throws Exception {
        StartupProperties.get().setProperty("falcon.postprocessing.enable", "true");
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, secureOption);

        final String umask = "000";

        FileSystem fs = trgMiniDFS.getFileSystem();
        Configuration conf = fs.getConf();
        conf.set("fs.permissions.umask-mode", umask);

        // ClusterHelper constructs new fs Conf. Add it to cluster properties so that it gets added to FS conf
        setUmaskInFsConf(trgCluster, umask);

        org.apache.falcon.entity.v0.feed.Cluster cluster = FeedHelper.getCluster(tableFeed, trgCluster.getName());
        final Calendar instance = Calendar.getInstance();
        instance.add(Calendar.YEAR, 1);
        cluster.getValidity().setEnd(instance.getTime());

        OozieCoordinatorBuilder builder = OozieCoordinatorBuilder.get(tableFeed, Tag.RETENTION);
        List<Properties> coords = builder.buildCoords(trgCluster, new Path("/projects/falcon/"));
        COORDINATORAPP coord = getCoordinator(trgMiniDFS, coords.get(0).getProperty(OozieEntityBuilder.ENTITY_PATH));

        Assert.assertEquals(coord.getAction().getWorkflow().getAppPath(), "${nameNode}/projects/falcon/RETENTION");
        Assert.assertEquals(coord.getName(), "FALCON_FEED_RETENTION_" + tableFeed.getName());
        Assert.assertEquals(coord.getFrequency(), "${coord:hours(6)}");

        HashMap<String, String> props = getCoordProperties(coord);

        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);

        String feedDataPath = wfProps.get("feedDataPath");
        String storageType = wfProps.get("falconFeedStorageType");

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
        Assert.assertEquals(wfProps.get("feedNames"), tableFeed.getName());
        Assert.assertEquals(wfProps.get("feedInstancePaths"), "IGNORE");

        assertWorkflowRetries(coord);
        verifyBrokerProperties(srcCluster, wfProps);
        verifyEntityProperties(tableFeed, trgCluster,
                WorkflowExecutionContext.EntityOperations.DELETE, wfProps);

        Assert.assertTrue(Storage.TYPE.TABLE == FeedHelper.getStorageType(tableFeed, trgCluster));
        assertHCatCredentials(getWorkflowapp(trgMiniDFS.getFileSystem(), coord),
                coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", ""));

        try {
            verifyClusterLocationsUMask(trgCluster, fs);
            verifyWorkflowUMask(fs, coord, umask);
        } finally {
            cleanupWorkflowState(fs, coord);
            FileSystem.closeAll();
        }
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

    private void verifyClusterLocationsUMask(Cluster aCluster, FileSystem fs) throws IOException {
        String stagingLocation = ClusterHelper.getLocation(aCluster, ClusterLocationType.STAGING).getPath();
        Path stagingPath = new Path(stagingLocation);
        if (fs.exists(stagingPath)) {
            FileStatus fileStatus = fs.getFileStatus(stagingPath);
            Assert.assertEquals(fileStatus.getPermission().toShort(), 511);
        }

        String workingLocation = ClusterHelper.getLocation(aCluster, ClusterLocationType.WORKING).getPath();
        Path workingPath = new Path(workingLocation);
        if (fs.exists(workingPath)) {
            FileStatus fileStatus = fs.getFileStatus(workingPath);
            Assert.assertEquals(fileStatus.getPermission().toShort(), 493);
        }
    }

    private void verifyWorkflowUMask(FileSystem fs, COORDINATORAPP coord,
                                     String defaultUMask) throws IOException {
        Assert.assertEquals(fs.getConf().get("fs.permissions.umask-mode"), defaultUMask);

        String appPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        Path wfPath = new Path(appPath);
        FileStatus[] fileStatuses = fs.listStatus(wfPath);
        for (FileStatus fileStatus : fileStatuses) {
            Assert.assertEquals(fileStatus.getOwner(), CurrentUser.getProxyUGI().getShortUserName());

            final FsPermission permission = fileStatus.getPermission();
            if (!fileStatus.isDirectory()) {
                Assert.assertEquals(permission.toString(),
                        HadoopClientFactory.getFileDefaultPermission(fs.getConf()).toString());
            }
        }
    }

    private void cleanupWorkflowState(FileSystem fs, COORDINATORAPP coord) throws Exception {
        String appPath = coord.getAction().getWorkflow().getAppPath();
        Path wfPath = new Path(appPath.replace("${nameNode}", ""));
        fs.delete(wfPath, true);
    }

    private static void setUmaskInFsConf(Cluster cluster, String umask) {
        // ClusterHelper constructs new fs Conf. Add it to cluster properties so that it gets added to FS conf
        org.apache.falcon.entity.v0.cluster.Property property =
                new org.apache.falcon.entity.v0.cluster.Property();
        property.setName("fs.permissions.umask-mode");
        property.setValue(umask);
        cluster.getProperties().getProperties().add(property);
    }

    @Test
    public void testUserDefinedProperties() throws Exception {
        Map<String, String> suppliedProps = new HashMap<>();
        suppliedProps.put("custom.property", "custom value");
        suppliedProps.put("ENTITY_NAME", "MyEntity");

        OozieEntityBuilder builder = OozieEntityBuilder.get(lifecycleRetentionFeed);
        Path bundlePath = new Path("/projects/falcon/");
        Properties props = builder.build(trgCluster, bundlePath, suppliedProps);

        Assert.assertNotNull(props);
        Assert.assertEquals(props.get("ENTITY_NAME"), lifecycleRetentionFeed.getName());
        Assert.assertEquals(props.get("custom.property"), "custom value");
    }

}
