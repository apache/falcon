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

package org.apache.falcon.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Sla;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.jdbc.MonitoringJdbcStateStore;
import org.apache.falcon.tools.FalconStateStoreDBCLI;
import org.apache.falcon.util.StateStoreProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.File;
import java.util.Date;

/**
 * Test for SLA Alerts.
 */
public class EntitySLAAlertServiceTest extends AbstractTestBase {
    private static final String DB_BASE_DIR = "target/test-data/persistancedb";
    protected static String dbLocation = DB_BASE_DIR + File.separator + "data.db";
    protected static String url = "jdbc:derby:"+ dbLocation +";create=true";
    protected static final String DB_SQL_FILE = DB_BASE_DIR + File.separator + "out.sql";
    protected LocalFileSystem fs = new LocalFileSystem();

    private static MonitoringJdbcStateStore monitoringJdbcStateStore;
    private static FalconJPAService falconJPAService = FalconJPAService.get();

    protected int execDBCLICommands(String[] args) {
        return new FalconStateStoreDBCLI().run(args);
    }

    public void createDB(String file) {
        File sqlFile = new File(file);
        String[] argsCreate = { "create", "-sqlfile", sqlFile.getAbsolutePath(), "-run" };
        int result = execDBCLICommands(argsCreate);
        Assert.assertEquals(0, result);
        Assert.assertTrue(sqlFile.exists());

    }

    @BeforeClass
    public void setup() throws Exception{
        StateStoreProperties.get().setProperty(FalconJPAService.URL, url);
        Configuration localConf = new Configuration();
        fs.initialize(LocalFileSystem.getDefaultUri(localConf), localConf);
        fs.mkdirs(new Path(DB_BASE_DIR));
        createDB(DB_SQL_FILE);
        falconJPAService.init();
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        monitoringJdbcStateStore = new MonitoringJdbcStateStore();
    }

    @BeforeMethod
    public void init() {
        clear();
    }

    private void clear() {
        EntityManager em = FalconJPAService.get().getEntityManager();
        em.getTransaction().begin();
        try {
            Query query = em.createNativeQuery("delete from PENDING_INSTANCES");
            query.executeUpdate();
            query = em.createNativeQuery("delete from MONITORED_ENTITY");
            query.executeUpdate();
            query = em.createNativeQuery("delete from ENTITY_SLA_ALERTS");
            query.executeUpdate();

        } finally {
            em.getTransaction().commit();
            em.close();
        }
    }

    @Test(expectedExceptions = javax.persistence.NoResultException.class)
    public static void processSLALowCandidates() throws FalconException, InterruptedException{

        Date dateOne =  new Date(System.currentTimeMillis()-100000);
        monitoringJdbcStateStore.putPendingInstances("test-feed", "test-cluster", dateOne, EntityType.FEED.toString());
        org.apache.falcon.entity.v0.feed.Clusters cluster = new org.apache.falcon.entity.v0.feed.Clusters();
        org.apache.falcon.entity.v0.cluster.Cluster cluster1 = new org.apache.falcon.entity.v0.cluster.Cluster();
        cluster1.setName("test-cluster");
        Cluster testCluster = new Cluster();
        testCluster.setName("test-cluster");
        cluster.getClusters().add(testCluster);
        Feed mockEntity = new Feed();
        mockEntity.setName("test-feed");
        mockEntity.setClusters(cluster);
        cluster1.setColo("test-cluster");


        if (ConfigurationStore.get().get(EntityType.FEED, mockEntity.getName()) == null) {
            ConfigurationStore.get().publish(EntityType.FEED, mockEntity);
        }
        if (ConfigurationStore.get().get(EntityType.CLUSTER, cluster1.getName()) == null) {
            ConfigurationStore.get().publish(EntityType.CLUSTER, cluster1);
        }
        Sla sla = new Sla();
        Frequency frequencyLow = new Frequency("1", Frequency.TimeUnit.minutes);
        Frequency frequencyHigh = new Frequency("2", Frequency.TimeUnit.minutes);
        sla.setSlaLow(frequencyLow);
        sla.setSlaHigh(frequencyHigh);
        mockEntity.setSla(sla);

        EntitySLAAlertService.get().init();
        Thread.sleep(10*1000);
        Assert.assertTrue(monitoringJdbcStateStore.getEntityAlertInstance("test-feed", "test-cluster",
                dateOne, EntityType.FEED.toString()).getIsSLALowMissed());
    }

    @Test(expectedExceptions = javax.persistence.NoResultException.class)
    public static void processSLACandidateProcess() throws FalconException, InterruptedException{
        Date dateOne =  new Date(System.currentTimeMillis()-130000);

        monitoringJdbcStateStore.putPendingInstances("test-process", "test-cluster", dateOne,
                EntityType.PROCESS.name());
        EntitySLAAlertService.get().init();
        org.apache.falcon.entity.v0.process.Clusters cluster = new org.apache.falcon.entity.v0.process.Clusters();
        org.apache.falcon.entity.v0.cluster.Cluster processCluster = new org.apache.falcon.entity.v0.cluster.Cluster();
        processCluster.setName("test-cluster");
        org.apache.falcon.entity.v0.process.Cluster testCluster = new org.apache.falcon.entity.v0.process.Cluster();
        testCluster.setName("test-cluster");
        cluster.getClusters().add(testCluster);
        Process process =  new Process();
        process.setName("test-process");
        process.setClusters(cluster);
        processCluster.setColo("test-cluster");

        if (ConfigurationStore.get().get(EntityType.PROCESS, process.getName()) == null){
            ConfigurationStore.get().publish(EntityType.PROCESS, process);
        }
        if (ConfigurationStore.get().get(EntityType.CLUSTER, processCluster.getName()) == null) {
            ConfigurationStore.get().publish(EntityType.CLUSTER, processCluster);
        }
        org.apache.falcon.entity.v0.process.Sla sla = new org.apache.falcon.entity.v0.process.Sla();
        Frequency processFrequency = new Frequency("1", Frequency.TimeUnit.minutes);
        sla.setShouldEndIn(processFrequency);
        process.setSla(sla);


        Thread.sleep(10*1000);
        Assert.assertTrue(monitoringJdbcStateStore.getEntityAlertInstance("test-process", "test-cluster", dateOne,
                EntityType.PROCESS.name()).getIsSLAHighMissed());

    }

    @Test(expectedExceptions = javax.persistence.NoResultException.class)
    public static void processSLAHighCandidates() throws FalconException, InterruptedException{

        Date dateOne =  new Date(System.currentTimeMillis()-130000);
        monitoringJdbcStateStore.putPendingInstances("test-feed", "test-cluster", dateOne, EntityType.FEED.toString());
        org.apache.falcon.entity.v0.feed.Clusters cluster = new org.apache.falcon.entity.v0.feed.Clusters();
        org.apache.falcon.entity.v0.cluster.Cluster cluster1 = new org.apache.falcon.entity.v0.cluster.Cluster();
        cluster1.setName("test-cluster");
        Cluster testCluster = new Cluster();
        testCluster.setName("test-cluster");
        cluster.getClusters().add(testCluster);
        Feed mockEntity = new Feed();
        mockEntity.setName("test-feed");
        mockEntity.setClusters(cluster);
        cluster1.setColo("test-cluster");
        if (ConfigurationStore.get().get(EntityType.FEED, mockEntity.getName()) == null) {
            ConfigurationStore.get().publish(EntityType.FEED, mockEntity);
        }
        if (ConfigurationStore.get().get(EntityType.CLUSTER, cluster1.getName()) == null) {
            ConfigurationStore.get().publish(EntityType.CLUSTER, cluster1);
        }
        Sla sla = new Sla();
        Frequency frequencyLow = new Frequency("1", Frequency.TimeUnit.minutes);
        Frequency frequencyHigh = new Frequency("2", Frequency.TimeUnit.minutes);
        sla.setSlaLow(frequencyLow);
        sla.setSlaHigh(frequencyHigh);
        mockEntity.setSla(sla);

        EntitySLAAlertService.get().init();
        Thread.sleep(10*1000);
        Assert.assertTrue(monitoringJdbcStateStore.getEntityAlertInstance("test-feed", "test-cluster",
                dateOne, EntityType.FEED.toString()).getIsSLAHighMissed());
    }
}
