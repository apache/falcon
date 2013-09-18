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
package org.apache.falcon.converter;

import static org.testng.Assert.assertEquals;

import java.util.Collection;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.coordinator.CONFIGURATION.Property;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for Oozie workflow definition for feed replication & retention.
 */
public class OozieFeedMapperTest {
    private EmbeddedCluster srcMiniDFS;
    private EmbeddedCluster trgMiniDFS;
    private final ConfigurationStore store = ConfigurationStore.get();
    private Cluster srcCluster;
    private Cluster trgCluster;
    private Feed feed;

    private static final String SRC_CLUSTER_PATH = "/src-cluster.xml";
    private static final String TRG_CLUSTER_PATH = "/trg-cluster.xml";
    private static final String FEED = "/feed.xml";

    @BeforeClass
    public void setUpDFS() throws Exception {
        srcMiniDFS = EmbeddedCluster.newCluster("cluster1");
        String srcHdfsUrl = srcMiniDFS.getConf().get("fs.default.name");

        trgMiniDFS = EmbeddedCluster.newCluster("cluster2");
        String trgHdfsUrl = trgMiniDFS.getConf().get("fs.default.name");

        cleanupStore();

        srcCluster = (Cluster) storeEntity(EntityType.CLUSTER, SRC_CLUSTER_PATH, srcHdfsUrl);

        trgCluster = (Cluster) storeEntity(EntityType.CLUSTER, TRG_CLUSTER_PATH, trgHdfsUrl);

        feed = (Feed) storeEntity(EntityType.FEED, FEED, null);

    }

    protected Entity storeEntity(EntityType type, String template, String writeEndpoint) throws Exception {
        Unmarshaller unmarshaller = type.getUnmarshaller();
        Entity entity = (Entity) unmarshaller
                .unmarshal(OozieFeedMapperTest.class.getResource(template));
        store.publish(type, entity);

        if (type == EntityType.CLUSTER) {
            Cluster cluster = (Cluster) entity;
            ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(writeEndpoint);
            FileSystem fs = new Path(writeEndpoint).getFileSystem(new Configuration());
            fs.create(new Path(ClusterHelper.getLocation(cluster, "working"), "libext/FEED/retention/ext.jar")).close();
            fs.create(
                    new Path(ClusterHelper.getLocation(cluster, "working"), "libext/FEED/replication/ext.jar")).close();
        }
        return entity;
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
    public void testFeedCoords() throws Exception {
        OozieFeedMapper feedMapper = new OozieFeedMapper(feed);
        List<COORDINATORAPP> coords = feedMapper.getCoordinators(trgCluster,
                new Path("/projects/falcon/"));
        //Assert retention coord
        COORDINATORAPP coord = coords.get(0);
        assertLibExtensions(coord, "retention");

        //Assert replication coord
        coord = coords.get(1);
        Assert.assertEquals("2010-01-01T00:40Z", coord.getStart());
        Assert.assertEquals("${nameNode}/projects/falcon/REPLICATION", coord
                .getAction().getWorkflow().getAppPath());
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
        Assert.assertEquals(
                "${nameNode}"
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

        for (Property prop : coord.getAction().getWorkflow().getConfiguration().getProperty()) {
            if (prop.getName().equals("mapred.job.priority")) {
                assertEquals(prop.getValue(), "NORMAL");
                break;
            }
        }
        assertLibExtensions(coord, "replication");
    }

    private void assertLibExtensions(COORDINATORAPP coord, String lifecycle) throws Exception {
        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        JAXBContext jaxbContext = JAXBContext.newInstance(WORKFLOWAPP.class);
        WORKFLOWAPP wf = ((JAXBElement<WORKFLOWAPP>) jaxbContext.createUnmarshaller().unmarshal(
                trgMiniDFS.getFileSystem().open(new Path(wfPath, "workflow.xml")))).getValue();
        List<Object> actions = wf.getDecisionOrForkOrJoin();
        for (Object obj : actions) {
            if (!(obj instanceof ACTION)) {
                continue;
            }
            ACTION action = (ACTION) obj;
            List<String> files = null;
            if (action.getJava() != null) {
                files = action.getJava().getFile();
            } else if (action.getPig() != null) {
                files = action.getPig().getFile();
            } else if (action.getMapReduce() != null) {
                files = action.getMapReduce().getFile();
            }
            if (files != null) {
                Assert.assertTrue(files.get(files.size() - 1).endsWith("/projects/falcon/working/libext/FEED/"
                        + lifecycle + "/ext.jar"));
            }
        }
    }
}
