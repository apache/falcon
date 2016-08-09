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

package org.apache.falcon.oozie.process;

import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.OozieEntityBuilder;
import org.apache.falcon.oozie.bundle.BUNDLEAPP;
import org.apache.falcon.oozie.bundle.CONFIGURATION;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.PIG;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBElement;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test for the Falcon entities mapping into Oozie artifacts.
 */
public class OozieProcessWorkflowBuilderTest extends AbstractTestBase {
    private static final String PROCESS_XML = "/config/process/process-0.1.xml";
    private static final String FEED_XML = "/config/feed/feed-0.1.xml";
    private static final String CLUSTER_XML = "/config/cluster/cluster-0.1.xml";
    private static final String PIG_PROCESS_XML = "/config/process/pig-process-0.1.xml";
    private static final String SPARK_PROCESS_XML = "/config/process/spark-process-0.1.xml";

    private String hdfsUrl;
    private FileSystem fs;
    private Cluster cluster;

    @BeforeClass
    public void setUpDFS() throws Exception {
        CurrentUser.authenticate(FalconTestUtil.TEST_USER_1);

        Configuration conf = EmbeddedCluster.newCluster("testCluster").getConf();
        hdfsUrl = conf.get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
    }

    private void storeEntity(EntityType type, String name, String resource) throws Exception {
        storeEntity(type, name, resource, null);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        storeEntity(EntityType.CLUSTER, "corp", CLUSTER_XML);
        storeEntity(EntityType.FEED, "clicks", FEED_XML);
        storeEntity(EntityType.FEED, "impressions", FEED_XML);
        storeEntity(EntityType.FEED, "clicksummary", FEED_XML);
        storeEntity(EntityType.PROCESS, "clicksummary", PROCESS_XML);
        storeEntity(EntityType.PROCESS, "pig-process", PIG_PROCESS_XML);


        ConfigurationStore store = ConfigurationStore.get();
        cluster = store.get(EntityType.CLUSTER, "corp");
        org.apache.falcon.entity.v0.cluster.Property property =
                new org.apache.falcon.entity.v0.cluster.Property();
        property.setName(SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
        property.setValue("hive/_HOST");
        cluster.getProperties().getProperties().add(property);

        ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(hdfsUrl);
        ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY).setEndpoint("thrift://localhost:49083");
        fs = new Path(hdfsUrl).getFileSystem(EmbeddedCluster.newConfiguration());
        fs.create(new Path(ClusterHelper.getLocation(cluster, ClusterLocationType.WORKING).getPath(),
                "libext/PROCESS/ext.jar")).close();

        Process process = store.get(EntityType.PROCESS, "clicksummary");
        Path wfpath = new Path(process.getWorkflow().getPath());
        assert new Path(hdfsUrl).getFileSystem(EmbeddedCluster.newConfiguration()).mkdirs(wfpath);
    }

    public void testDefCoordMap(Process process, COORDINATORAPP coord) throws Exception {
        assertEquals("FALCON_PROCESS_DEFAULT_" + process.getName(), coord.getName());
        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        assertEquals(SchemaHelper.formatDateUTC(processValidity.getStart()), coord.getStart());
        assertEquals(SchemaHelper.formatDateUTC(processValidity.getEnd()), coord.getEnd());
        assertEquals("${coord:" + process.getFrequency().toString() + "}", coord.getFrequency());
        assertEquals(process.getTimezone().getID(), coord.getTimezone());

        assertEquals(process.getParallel() + "", coord.getControls().getConcurrency());
        assertEquals(process.getOrder().name(), coord.getControls().getExecution());

        assertEquals(process.getInputs().getInputs().get(0).getName(),
                coord.getInputEvents().getDataIn().get(0).getName());
        assertEquals(process.getInputs().getInputs().get(0).getName(),
                coord.getInputEvents().getDataIn().get(0).getDataset());
        assertEquals("${" + process.getInputs().getInputs().get(0).getStart() + "}",
                coord.getInputEvents().getDataIn().get(0).getStartInstance());
        assertEquals("${" + process.getInputs().getInputs().get(0).getEnd() + "}",
                coord.getInputEvents().getDataIn().get(0).getEndInstance());

        assertEquals(process.getInputs().getInputs().get(1).getName(),
                coord.getInputEvents().getDataIn().get(1).getName());
        assertEquals(process.getInputs().getInputs().get(1).getName(),
                coord.getInputEvents().getDataIn().get(1).getDataset());
        assertEquals("${" + process.getInputs().getInputs().get(1).getStart() + "}",
                coord.getInputEvents().getDataIn().get(1).getStartInstance());
        assertEquals("${" + process.getInputs().getInputs().get(1).getEnd() + "}",
                coord.getInputEvents().getDataIn().get(1).getEndInstance());

        assertEquals(process.getOutputs().getOutputs().get(0).getName() + "stats",
                coord.getOutputEvents().getDataOut().get(1).getName());
        assertEquals(process.getOutputs().getOutputs().get(0).getName() + "meta",
                coord.getOutputEvents().getDataOut().get(2).getName());

        assertEquals(process.getOutputs().getOutputs().get(0).getName(),
                coord.getOutputEvents().getDataOut().get(0).getName());
        assertEquals("${" + process.getOutputs().getOutputs().get(0).getInstance() + "}",
                coord.getOutputEvents().getDataOut().get(0).getInstance());
        assertEquals(process.getOutputs().getOutputs().get(0).getName(),
                coord.getOutputEvents().getDataOut().get(0).getDataset());

        assertEquals(5, coord.getDatasets().getDatasetOrAsyncDataset().size());

        ConfigurationStore store = ConfigurationStore.get();
        Feed feed = store.get(EntityType.FEED, process.getInputs().getInputs().get(0).getFeed());
        SYNCDATASET ds = (SYNCDATASET) coord.getDatasets().getDatasetOrAsyncDataset().get(0);

        final org.apache.falcon.entity.v0.feed.Cluster feedCluster = feed.getClusters().getClusters().get(0);
        assertEquals(SchemaHelper.formatDateUTC(feedCluster.getValidity().getStart()), ds.getInitialInstance());
        assertEquals(feed.getTimezone().getID(), ds.getTimezone());
        assertEquals("${coord:" + feed.getFrequency().toString() + "}", ds.getFrequency());
        assertEquals("", ds.getDoneFlag());
        assertEquals(ds.getUriTemplate(),
                FeedHelper.createStorage(feedCluster, feed).getUriTemplate(LocationType.DATA));

        HashMap<String, String> props = getCoordProperties(coord);
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);
        assertEquals(wfProps.get("mapred.job.priority"), "LOW");
        List<Input> inputs = process.getInputs().getInputs();
        assertEquals(props.get(WorkflowExecutionArgs.INPUT_NAMES.getName()), inputs.get(0).getName() + "#" + inputs
            .get(1).getName());

        verifyEntityProperties(process, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        assertLibExtensions(fs, coord, EntityType.PROCESS, null);
    }

    @Test
    public void testBundle() throws Exception {
        String path = StartupProperties.get().getProperty("system.lib.location");
        if (!new File(path).exists()) {
            Assert.assertTrue(new File(path).mkdirs());
        }
        Process process = ConfigurationStore.get().get(EntityType.PROCESS, "clicksummary");

        WORKFLOWAPP parentWorkflow = initializeProcessMapper(process, "12", "360");
        testParentWorkflow(process, parentWorkflow);

        ACTION oozieAction = getAction(parentWorkflow, "user-action");
        Assert.assertNotNull(oozieAction.getSubWorkflow());
    }

    @Test
    public void testBundle1() throws Exception {
        Process process = ConfigurationStore.get().get(EntityType.PROCESS, "clicksummary");
        process.setFrequency(Frequency.fromString("minutes(1)"));
        process.setTimeout(Frequency.fromString("minutes(15)"));

        WORKFLOWAPP parentWorkflow = initializeProcessMapper(process, "30", "15");
        testParentWorkflow(process, parentWorkflow);
    }

    @Test
    public void testPigProcessMapper() throws Exception {
        Process process = ConfigurationStore.get().get(EntityType.PROCESS, "pig-process");
        Assert.assertEquals("pig", process.getWorkflow().getEngine().value());

        prepare(process);
        WORKFLOWAPP parentWorkflow = initializeProcessMapper(process, "12", "360");
        testParentWorkflow(process, parentWorkflow);

        ACTION pigActionNode = getAction(parentWorkflow, "user-action");

        final PIG pigAction = pigActionNode.getPig();
        Assert.assertEquals(pigAction.getScript(), "${nameNode}/apps/pig/id.pig");
        Assert.assertNotNull(pigAction.getPrepare());
        Assert.assertEquals(1, pigAction.getPrepare().getDelete().size());
        Assert.assertFalse(pigAction.getParam().isEmpty());
        Assert.assertEquals(5, pigAction.getParam().size());
        Assert.assertEquals(Collections.EMPTY_LIST, pigAction.getArchive());
        Assert.assertTrue(pigAction.getFile().size() > 0);
    }

    @DataProvider(name = "secureOptions")
    private Object[][] createOptions() {
        return new Object[][] {
            {"simple"},
            {"kerberos"},
        };
    }

    @Test (dataProvider = "secureOptions")
    public void testHiveProcessMapper(String secureOption) throws Exception {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, secureOption);

        URL resource = this.getClass().getResource("/config/feed/hive-table-feed.xml");
        Feed inFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.FEED, inFeed);

        resource = this.getClass().getResource("/config/feed/hive-table-feed-out.xml");
        Feed outFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.FEED, outFeed);

        resource = this.getClass().getResource("/config/process/hive-process.xml");
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, process);

        prepare(process);
        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(process).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> props = getCoordProperties(coord);
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);

        verifyEntityProperties(process, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        // verify table and hive props
        Map<String, String> expected = getExpectedProperties(inFeed, outFeed, process);
        expected.putAll(ClusterHelper.getHiveProperties(cluster));
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (expected.containsKey(entry.getKey())) {
                Assert.assertEquals(entry.getValue(), expected.get(entry.getKey()));
            }
        }

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP parentWorkflow = getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));
        testParentWorkflow(process, parentWorkflow);

        ACTION hiveNode = getAction(parentWorkflow, "user-action");

        JAXBElement<org.apache.falcon.oozie.hive.ACTION> actionJaxbElement = OozieUtils.unMarshalHiveAction(hiveNode);
        org.apache.falcon.oozie.hive.ACTION hiveAction = actionJaxbElement.getValue();

        Assert.assertEquals(hiveAction.getScript(), "${nameNode}/apps/hive/script.hql");
        Assert.assertEquals(hiveAction.getJobXml(), "${wf:appPath()}/conf/hive-site.xml");
        Assert.assertNull(hiveAction.getPrepare());
        Assert.assertEquals(Collections.EMPTY_LIST, hiveAction.getArchive());
        Assert.assertFalse(hiveAction.getParam().isEmpty());
        Assert.assertEquals(14, hiveAction.getParam().size());

        Assert.assertTrue(Storage.TYPE.TABLE == ProcessHelper.getStorageType(cluster, process));
        assertHCatCredentials(parentWorkflow, wfPath);

        ConfigurationStore.get().remove(EntityType.PROCESS, process.getName());
    }

    @Test
    public void testSparkSQLProcess() throws Exception {
        URL resource = this.getClass().getResource("/config/feed/hive-table-feed.xml");
        Feed inFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.FEED, inFeed);

        resource = this.getClass().getResource("/config/feed/hive-table-feed-out.xml");
        Feed outFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.FEED, outFeed);

        resource = this.getClass().getResource("/config/process/spark-sql-process.xml");
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, process);

        prepare(process);
        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(process).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> props = getCoordProperties(coord);
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);

        verifyEntityProperties(process, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        // verify table and hive props
        Map<String, String> expected = getExpectedProperties(inFeed, outFeed, process);
        expected.putAll(ClusterHelper.getHiveProperties(cluster));
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (expected.containsKey(entry.getKey())) {
                Assert.assertEquals(entry.getValue(), expected.get(entry.getKey()));
            }
        }

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP parentWorkflow = getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));
        testParentWorkflow(process, parentWorkflow);
        assertEquals(process.getWorkflow().getLib(), "/resources/action/lib/falcon-examples.jar");

        ACTION sparkNode = getAction(parentWorkflow, "user-action");

        JAXBElement<org.apache.falcon.oozie.spark.ACTION> actionJaxbElement =
                OozieUtils.unMarshalSparkAction(sparkNode);
        org.apache.falcon.oozie.spark.ACTION sparkAction = actionJaxbElement.getValue();

        assertEquals(sparkAction.getMaster(), "local");
        assertEquals(sparkAction.getJar(), "falcon-examples.jar");

        Assert.assertTrue(Storage.TYPE.TABLE == ProcessHelper.getStorageType(cluster, process));
        List<String> argsList = sparkAction.getArg();

        Input input = process.getInputs().getInputs().get(0);
        Output output = process.getOutputs().getOutputs().get(0);

        assertEquals(argsList.get(0), "${falcon_"+input.getName()+"_partition_filter_hive}");
        assertEquals(argsList.get(1), "${falcon_"+input.getName()+"_table}");
        assertEquals(argsList.get(2), "${falcon_"+input.getName()+"_database}");
        assertEquals(argsList.get(3), "${falcon_"+output.getName()+"_partitions_hive}");
        assertEquals(argsList.get(4), "${falcon_"+output.getName()+"_table}");
        assertEquals(argsList.get(5), "${falcon_"+output.getName()+"_database}");

        ConfigurationStore.get().remove(EntityType.PROCESS, process.getName());
    }

    @Test
    public void testSparkProcess() throws Exception {

        URL resource = this.getClass().getResource(SPARK_PROCESS_XML);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, process);
        Assert.assertEquals("spark", process.getWorkflow().getEngine().value());

        prepare(process);
        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(process).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> props = getCoordProperties(coord);
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);

        verifyEntityProperties(process, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP parentWorkflow = getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));
        testParentWorkflow(process, parentWorkflow);
        assertEquals(process.getWorkflow().getLib(), "/resources/action/lib/spark-wordcount.jar");

        ACTION sparkNode = getAction(parentWorkflow, "user-action");

        JAXBElement<org.apache.falcon.oozie.spark.ACTION> actionJaxbElement =
                OozieUtils.unMarshalSparkAction(sparkNode);
        org.apache.falcon.oozie.spark.ACTION sparkAction = actionJaxbElement.getValue();
        assertEquals(sparkAction.getMaster(), "local");
        assertEquals(sparkAction.getJar(), "spark-wordcount.jar");
        List<String> argsList = sparkAction.getArg();
        Input input = process.getInputs().getInputs().get(0);
        Output output = process.getOutputs().getOutputs().get(0);
        assertEquals(argsList.get(0), "${"+input.getName().toString()+"}");
        assertEquals(argsList.get(argsList.size()-1), "${"+output.getName().toString()+"}");
    }

    @Test (dataProvider = "secureOptions")
    public void testHiveProcessMapperWithFSInputFeedAndTableOutputFeed(String secureOption) throws Exception {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, secureOption);

        URL resource = this.getClass().getResource("/config/feed/hive-table-feed-out.xml");
        Feed outFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.FEED, outFeed);

        resource = this.getClass().getResource("/config/process/hive-process-FSInputFeed.xml");
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, process);

        prepare(process);
        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(process).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);

        verifyEntityProperties(process, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP parentWorkflow = getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));
        testParentWorkflow(process, parentWorkflow);

        ACTION hiveNode = getAction(parentWorkflow, "user-action");

        JAXBElement<org.apache.falcon.oozie.hive.ACTION> actionJaxbElement = OozieUtils.unMarshalHiveAction(hiveNode);
        org.apache.falcon.oozie.hive.ACTION hiveAction = actionJaxbElement.getValue();

        Assert.assertEquals(hiveAction.getScript(), "${nameNode}/apps/hive/script.hql");
        Assert.assertEquals(hiveAction.getJobXml(), "${wf:appPath()}/conf/hive-site.xml");
        Assert.assertNull(hiveAction.getPrepare());
        Assert.assertEquals(Collections.EMPTY_LIST, hiveAction.getArchive());
        Assert.assertFalse(hiveAction.getParam().isEmpty());
        Assert.assertEquals(10, hiveAction.getParam().size());

        Assert.assertTrue(Storage.TYPE.TABLE == ProcessHelper.getStorageType(cluster, process));
        assertHCatCredentials(parentWorkflow, wfPath);

        ConfigurationStore.get().remove(EntityType.PROCESS, process.getName());
    }

    @Test (dataProvider = "secureOptions")
    public void testHiveProcessMapperWithTableInputFeedAndFSOutputFeed(String secureOption) throws Exception {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, secureOption);

        URL resource = this.getClass().getResource("/config/feed/hive-table-feed.xml");
        Feed inFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.FEED, inFeed);

        resource = this.getClass().getResource("/config/process/hive-process-FSOutputFeed.xml");
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, process);

        prepare(process);
        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(process).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);

        verifyEntityProperties(process, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP parentWorkflow = getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));
        testParentWorkflow(process, parentWorkflow);

        ACTION hiveNode = getAction(parentWorkflow, "user-action");

        JAXBElement<org.apache.falcon.oozie.hive.ACTION> actionJaxbElement = OozieUtils.unMarshalHiveAction(hiveNode);
        org.apache.falcon.oozie.hive.ACTION hiveAction = actionJaxbElement.getValue();

        Assert.assertEquals(hiveAction.getScript(), "${nameNode}/apps/hive/script.hql");
        Assert.assertEquals(hiveAction.getJobXml(), "${wf:appPath()}/conf/hive-site.xml");
        Assert.assertNotNull(hiveAction.getPrepare());
        Assert.assertEquals(Collections.EMPTY_LIST, hiveAction.getArchive());
        Assert.assertFalse(hiveAction.getParam().isEmpty());
        Assert.assertEquals(6, hiveAction.getParam().size());

        Assert.assertTrue(Storage.TYPE.TABLE == ProcessHelper.getStorageType(cluster, process));
        assertHCatCredentials(parentWorkflow, wfPath);

        ConfigurationStore.get().remove(EntityType.PROCESS, process.getName());
    }

    @Test (dataProvider = "secureOptions")
    public void testHiveProcessWithNoInputsAndOutputs(String secureOption) throws Exception {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, secureOption);

        URL resource = this.getClass().getResource("/config/process/dumb-hive-process.xml");
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, process);

        prepare(process);
        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(process).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);
        verifyEntityProperties(process, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP parentWorkflow = getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));
        testParentWorkflow(process, parentWorkflow);

        ACTION hiveNode = getAction(parentWorkflow, "user-action");

        JAXBElement<org.apache.falcon.oozie.hive.ACTION> actionJaxbElement = OozieUtils.unMarshalHiveAction(
                hiveNode);
        org.apache.falcon.oozie.hive.ACTION hiveAction = actionJaxbElement.getValue();

        Assert.assertEquals(hiveAction.getScript(), "${nameNode}/apps/hive/script.hql");
        Assert.assertEquals(hiveAction.getJobXml(), "${wf:appPath()}/conf/hive-site.xml");
        Assert.assertNull(hiveAction.getPrepare());
        Assert.assertEquals(Collections.EMPTY_LIST, hiveAction.getArchive());
        Assert.assertTrue(hiveAction.getParam().isEmpty());

        ConfigurationStore.get().remove(EntityType.PROCESS, process.getName());
    }

    private void assertHCatCredentials(WORKFLOWAPP wf, String wfPath) throws IOException {
        Path hiveConfPath = new Path(new Path(wfPath), "conf/hive-site.xml");
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

            if (!SecurityUtil.isSecurityEnabled()) {
                Assert.assertNull(action.getCred());
                return;
            }

            String actionName = action.getName();
            if ("user-hive-job".equals(actionName) || "user-pig-job".equals(actionName)
                    || "user-oozie-workflow".equals(actionName) || "recordsize".equals(actionName)) {
                Assert.assertNotNull(action.getCred());
                Assert.assertEquals(action.getCred(), "falconHiveAuth");
            }
        }
    }

    private void prepare(Process process) throws IOException {
        Path wf = new Path(process.getWorkflow().getPath());
        fs.mkdirs(wf.getParent());
        fs.create(wf).close();
    }

    @Test (dataProvider = "secureOptions")
    public void testProcessMapperForTableStorage(String secureOption) throws Exception {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, secureOption);

        URL resource = this.getClass().getResource("/config/feed/hive-table-feed.xml");
        Feed inFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.FEED, inFeed);

        resource = this.getClass().getResource("/config/feed/hive-table-feed-out.xml");
        Feed outFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.FEED, outFeed);

        resource = this.getClass().getResource("/config/process/pig-process-table.xml");
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, process);

        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(process).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> props = getCoordProperties(coord);

        // verify table props
        Map<String, String> expected = getExpectedProperties(inFeed, outFeed, process);
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (expected.containsKey(entry.getKey())) {
                Assert.assertEquals(entry.getValue(), expected.get(entry.getKey()));
            }
        }

        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);
        verifyEntityProperties(process, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        // verify the late data params
        Assert.assertEquals(props.get("falconInputFeeds"), process.getInputs().getInputs().get(0).getFeed());
        Assert.assertEquals(props.get("falconInPaths"), "${coord:dataIn('input')}");
        Assert.assertEquals(props.get("falconInputFeedStorageTypes"), Storage.TYPE.TABLE.name());
        Assert.assertEquals(props.get(WorkflowExecutionArgs.INPUT_NAMES.getName()),
            process.getInputs().getInputs().get(0).getName());

        // verify the post processing params
        Assert.assertEquals(props.get("feedNames"), process.getOutputs().getOutputs().get(0).getFeed());
        Assert.assertEquals(props.get("feedInstancePaths"), "${coord:dataOut('output')}");

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP parentWorkflow = getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));

        Assert.assertTrue(Storage.TYPE.TABLE == ProcessHelper.getStorageType(cluster, process));
        assertHCatCredentials(parentWorkflow, wfPath);
    }

    private Map<String, String> getExpectedProperties(Feed inFeed, Feed outFeed,
                                                      Process process) throws FalconException {
        Map<String, String> expected = new HashMap<String, String>();
        if (process.getInputs() != null) {
            for (Input input : process.getInputs().getInputs()) {
                CatalogStorage storage = (CatalogStorage) FeedHelper.createStorage(cluster, inFeed);
                propagateStorageProperties(input.getName(), storage, expected);
            }
        }

        if (process.getOutputs() != null) {
            for (Output output : process.getOutputs().getOutputs()) {
                CatalogStorage storage = (CatalogStorage) FeedHelper.createStorage(cluster, outFeed);
                propagateStorageProperties(output.getName(), storage, expected);
            }
        }

        return expected;
    }

    private void propagateStorageProperties(String feedName, CatalogStorage tableStorage,
                                            Map<String, String> props) {
        String prefix = "falcon_" + feedName;
        props.put(prefix + "_storage_type", tableStorage.getType().name());
        props.put(prefix + "_catalog_url", tableStorage.getCatalogUrl());
        props.put(prefix + "_database", tableStorage.getDatabase());
        props.put(prefix + "_table", tableStorage.getTable());

        if (prefix.equals("falcon_input")) {
            props.put(prefix + "_partition_filter_pig", "${coord:dataInPartitionFilter('input', 'pig')}");
            props.put(prefix + "_partition_filter_hive", "${coord:dataInPartitionFilter('input', 'hive')}");
            props.put(prefix + "_partition_filter_java", "${coord:dataInPartitionFilter('input', 'java')}");
        } else if (prefix.equals("falcon_output")) {
            props.put(prefix + "_dataout_partitions", "${coord:dataOutPartitions('output')}");
        }
    }

    @Test
    public void testProcessWorkflowMapper() throws Exception {
        Process process = ConfigurationStore.get().get(EntityType.PROCESS, "clicksummary");
        Workflow processWorkflow = process.getWorkflow();
        Assert.assertEquals("test", processWorkflow.getName());
        Assert.assertEquals("1.0.0", processWorkflow.getVersion());
    }

    private WORKFLOWAPP initializeProcessMapper(Process process, String throttle, String timeout)
        throws Exception {
        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(process).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(),
            bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");
        List<CONFIGURATION.Property> props = bundle.getCoordinator().get(0).getConfiguration().getProperty();
        for (CONFIGURATION.Property prop : props) {
            if (prop.getName().equals("oozie.libpath")) {
                Assert.assertEquals(prop.getValue().replace("${nameNode}", ""), process.getWorkflow().getLib());
            }
        }

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        testDefCoordMap(process, coord);
        assertEquals(coord.getControls().getThrottle(), throttle);
        assertEquals(coord.getControls().getTimeout(), timeout);

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        return getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));
    }

    public void testParentWorkflow(Process process, WORKFLOWAPP parentWorkflow) {
        Assert.assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString(), parentWorkflow.getName());

        if (process.getLateProcess() != null) {
            assertAction(parentWorkflow, "pre-processing", true);
        }
        assertAction(parentWorkflow, "succeeded-post-processing", true);
        assertAction(parentWorkflow, "failed-post-processing", true);
        assertAction(parentWorkflow, "user-action", false);
    }

    @Test
    public void testPostProcessingProcess() throws Exception {
        StartupProperties.get().setProperty("falcon.postprocessing.enable", "false");
        Process process = ConfigurationStore.get().get(EntityType.PROCESS, "pig-process");

        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Path bundlePath = new Path("/falcon/staging/workflows", process.getName());
        builder.build(cluster, bundlePath);
        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");
        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));

        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP workflowapp = getWorkflowapp(fs, new Path(wfPath, "workflow.xml"));

        Boolean foudUserAction = false;
        Boolean foundpostProcessing =false;

        for(Object action : workflowapp.getDecisionOrForkOrJoin()){
            if (action instanceof ACTION && ((ACTION)action).getName().equals("user-action")){
                foudUserAction = true;
            }
            if (action instanceof ACTION && ((ACTION)action).getName().contains("post")){
                foundpostProcessing = true;
            }

        }
        assertTrue(foudUserAction);
        assertFalse(foundpostProcessing);
        StartupProperties.get().setProperty("falcon.postprocessing.enable", "true");
    }

    @AfterMethod
    public void cleanup() throws Exception {
        cleanupStore();
    }

    @Test
    public void testProcessWithNoInputsAndOutputs() throws Exception {
        ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(hdfsUrl);

        URL resource = this.getClass().getResource("/config/process/dumb-process.xml");
        Process processEntity = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, processEntity);

        OozieEntityBuilder builder = OozieEntityBuilder.get(processEntity);
        Path bundlePath = new Path("/falcon/staging/workflows", processEntity.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(processEntity).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, processEntity).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> props = getCoordProperties(coord);
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);
        verifyEntityProperties(processEntity, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        String[] expected = {
            WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(),
            WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(),
            WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(),
            WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(),
            WorkflowExecutionArgs.INPUT_NAMES.getName(),
        };

        for (String property : expected) {
            Assert.assertTrue(props.containsKey(property), "expected property missing: " + property);
        }
    }

    @Test
    public void testProcessWithInputsNoOutputs() throws Exception {
        ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(hdfsUrl);

        URL resource = this.getClass().getResource("/config/process/process-no-outputs.xml");
        Process processEntity = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, processEntity);

        OozieEntityBuilder builder = OozieEntityBuilder.get(processEntity);
        Path bundlePath = new Path("/falcon/staging/workflows", processEntity.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(processEntity).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, processEntity).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> props = getCoordProperties(coord);
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);
        verifyEntityProperties(processEntity, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        Assert.assertEquals(props.get(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName()), "clicks");
        Assert.assertEquals(props.get(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName()), "NONE");
    }

    @Test
    public void testProcessNoInputsWithOutputs() throws Exception {
        ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(hdfsUrl);

        URL resource = this.getClass().getResource("/config/process/process-no-inputs.xml");
        Process processEntity = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(resource);
        ConfigurationStore.get().publish(EntityType.PROCESS, processEntity);

        OozieEntityBuilder builder = OozieEntityBuilder.get(processEntity);
        Path bundlePath = new Path("/falcon/staging/workflows", processEntity.getName());
        builder.build(cluster, bundlePath);
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(EntityUtil.getWorkflowName(processEntity).toString(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(EntityUtil.getWorkflowName(Tag.DEFAULT, processEntity).toString(),
                bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");

        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        HashMap<String, String> props = getCoordProperties(coord);
        HashMap<String, String> wfProps = getWorkflowProperties(fs, coord);
        verifyEntityProperties(processEntity, cluster,
                WorkflowExecutionContext.EntityOperations.GENERATE, wfProps);
        verifyBrokerProperties(cluster, wfProps);

        Assert.assertEquals(props.get(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName()), "impressions");
        Assert.assertEquals(props.get(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName()), "NONE");
    }

    @Test
    public void testUserDefinedProperties() throws Exception {
        Map<String, String> suppliedProps = new HashMap<>();
        suppliedProps.put("custom.property", "custom value");
        suppliedProps.put("ENTITY_NAME", "MyEntity");

        Process process = ConfigurationStore.get().get(EntityType.PROCESS, "clicksummary");
        Path bundlePath = new Path("/projects/falcon/");
        OozieEntityBuilder builder = OozieEntityBuilder.get(process);
        Properties props = builder.build(cluster, bundlePath, suppliedProps);

        Assert.assertNotNull(props);
        Assert.assertEquals(props.get("ENTITY_NAME"), process.getName());
        Assert.assertEquals(props.get("custom.property"), "custom value");
    }

}
