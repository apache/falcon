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

package org.apache.falcon.extensions;

import junit.framework.Assert;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.extensions.mirroring.hdfs.HdfsMirroringExtension;
import org.apache.falcon.extensions.mirroring.hdfs.HdfsMirroringExtensionProperties;
import org.apache.falcon.extensions.store.AbstractTestExtensionStore;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Tests for Extension.
 */
public class ExtensionTest extends AbstractTestExtensionStore {
    private static final String PRIMARY_CLUSTER_XML = "/primary-cluster-0.1.xml";
    private static final String BACKUP_CLUSTER_XML = "/backup-cluster-0.1.xml";
    private static final String JOB_NAME = "hdfs-mirroring-monthly";
    private static final String JOB_CLUSTER_NAME = "primaryCluster";
    private static final String VALIDITY_START = "2016-01-02T00:00Z";
    private static final String VALIDITY_END = "2018-01-02T00:00Z";
    private static final String FREQUENCY = "days(1)";
    private static final String SOURCEDIR = "/users/source/file1";
    private static final String SOURCE_CLUSTER = "primaryCluster";
    private static final String TARGETDIR = "/users/target/file1";
    private static final String TARGET_CLUSTER = "backupCluster";
    private Extension extension;

    private static Properties getHdfsProperties() {
        Properties properties = new Properties();
        properties.setProperty(ExtensionProperties.JOB_NAME.getName(),
                JOB_NAME);
        properties.setProperty(ExtensionProperties.CLUSTER_NAME.getName(),
                JOB_CLUSTER_NAME);
        properties.setProperty(ExtensionProperties.VALIDITY_START.getName(),
                VALIDITY_START);
        properties.setProperty(ExtensionProperties.VALIDITY_END.getName(),
                VALIDITY_END);
        properties.setProperty(ExtensionProperties.FREQUENCY.getName(),
                FREQUENCY);
        properties.setProperty(HdfsMirroringExtensionProperties.SOURCE_DIR.getName(),
                SOURCEDIR);
        properties.setProperty(HdfsMirroringExtensionProperties.SOURCE_CLUSTER.getName(),
                SOURCE_CLUSTER);
        properties.setProperty(HdfsMirroringExtensionProperties.TARGET_DIR.getName(),
                TARGETDIR);
        properties.setProperty(HdfsMirroringExtensionProperties.TARGET_CLUSTER.getName(),
                TARGET_CLUSTER);

        return properties;
    }

    @BeforeClass
    public void init() throws Exception {
        extension = new Extension();
        initClusters();
    }

    private void initClusters() throws Exception {
        InputStream inputStream = getClass().getResourceAsStream(PRIMARY_CLUSTER_XML);
        Cluster primaryCluster = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(inputStream);
        ConfigurationStore.get().publish(EntityType.CLUSTER, primaryCluster);

        inputStream = getClass().getResourceAsStream(BACKUP_CLUSTER_XML);
        Cluster backupCluster = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(inputStream);
        ConfigurationStore.get().publish(EntityType.CLUSTER, backupCluster);
    }

    @Test
    public void testGetExtensionEntitiesForHdfsMirroring() throws FalconException {
        ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory.getParser(EntityType.PROCESS);

        List<Entity> entities = extension.getEntities(new HdfsMirroringExtension().getName(), getHdfsProperties());
        if (entities == null || entities.isEmpty()) {
            Assert.fail("Entities returned cannot be null or empty");
        }

        Assert.assertEquals(1, entities.size());
        Entity entity = entities.get(0);


        Assert.assertEquals(EntityType.PROCESS, entity.getEntityType());
        parser.parse(new ByteArrayInputStream(entity.toString().getBytes()));

        // Validate
        Process processEntity = (Process) entity;
        Assert.assertEquals(JOB_NAME, processEntity.getName());
        org.apache.falcon.entity.v0.process.Cluster jobCluster = processEntity.getClusters().
                getClusters().get(0);
        Assert.assertEquals(JOB_CLUSTER_NAME, jobCluster.getName());
        Assert.assertEquals(VALIDITY_START, SchemaHelper.formatDateUTC(jobCluster.getValidity().getStart()));
        Assert.assertEquals(VALIDITY_END, SchemaHelper.formatDateUTC(jobCluster.getValidity().getEnd()));

        Assert.assertEquals(FREQUENCY, processEntity.getFrequency().toString());
        Assert.assertEquals("UTC", processEntity.getTimezone().getID());

        Assert.assertEquals(EngineType.OOZIE, processEntity.getWorkflow().getEngine());
        Assert.assertEquals(extensionStorePath + "/hdfs-mirroring/libs",
                processEntity.getWorkflow().getLib());
        Assert.assertEquals(extensionStorePath
                + "/hdfs-mirroring/resources/runtime/hdfs-mirroring-workflow.xml",
                processEntity.getWorkflow().getPath());

        Properties props = EntityUtil.getEntityProperties(processEntity);

        String srcClusterEndPoint = ClusterHelper.getReadOnlyStorageUrl(ClusterHelper.getCluster(SOURCE_CLUSTER));
        Assert.assertEquals(srcClusterEndPoint + SOURCEDIR, props.getProperty("sourceDir"));
        Assert.assertEquals(SOURCE_CLUSTER, props.getProperty("sourceCluster"));
        Assert.assertEquals(TARGETDIR, props.getProperty("targetDir"));
        Assert.assertEquals(TARGET_CLUSTER, props.getProperty("targetCluster"));

        //retry
        Assert.assertEquals(3, processEntity.getRetry().getAttempts());
        Assert.assertEquals(PolicyType.PERIODIC, processEntity.getRetry().getPolicy());
        Assert.assertEquals("minutes(30)", processEntity.getRetry().getDelay().toString());
    }

    @Test(expectedExceptions = FalconException.class,
            expectedExceptionsMessageRegExp = "Missing extension property: jobClusterName")
    public void testGetExtensionEntitiesForHdfsMirroringMissingMandatoryProperties() throws FalconException {
        Properties props = getHdfsProperties();
        props.remove(ExtensionProperties.CLUSTER_NAME.getName());

        extension.getEntities(new HdfsMirroringExtension().getName(), props);
    }
}
