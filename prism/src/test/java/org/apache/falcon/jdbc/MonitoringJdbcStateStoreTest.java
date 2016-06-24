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
package org.apache.falcon.jdbc;

import java.io.File;
import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.service.FalconJPAService;
import org.apache.falcon.tools.FalconStateStoreDBCLI;
import org.apache.falcon.util.StateStoreProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
*Unit test for MonitoringJdbcStateStore.
 * */

public class MonitoringJdbcStateStoreTest extends AbstractTestBase {
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

    @Test
    public void testInsertRetrieveAndUpdate() throws Exception {
        monitoringJdbcStateStore.putMonitoredFeed("test_feed1");
        monitoringJdbcStateStore.putMonitoredFeed("test_feed2");
        Assert.assertEquals("test_feed1", monitoringJdbcStateStore.getMonitoredFeed("test_feed1").getFeedName());
        Assert.assertEquals(monitoringJdbcStateStore.getAllMonitoredFeed().size(), 2);

        monitoringJdbcStateStore.deleteMonitoringFeed("test_feed1");
        monitoringJdbcStateStore.deleteMonitoringFeed("test_feed2");
        Date dateOne =  SchemaHelper.parseDateUTC("2015-11-20T00:00Z");
        Date dateTwo =  SchemaHelper.parseDateUTC("2015-11-20T01:00Z");
        monitoringJdbcStateStore.putPendingInstances("test_feed1", "test_cluster", dateOne);
        monitoringJdbcStateStore.putPendingInstances("test_feed1", "test_cluster", dateTwo);

        Assert.assertEquals(monitoringJdbcStateStore.getNominalInstances("test_feed1", "test_cluster").size(), 2);
        monitoringJdbcStateStore.deletePendingInstance("test_feed1", "test_cluster", dateOne);
        Assert.assertEquals(monitoringJdbcStateStore.getNominalInstances("test_feed1", "test_cluster").size(), 1);
        monitoringJdbcStateStore.deletePendingInstances("test_feed1", "test_cluster");
    }

    @Test
    public void testEmptyLatestInstance() throws Exception {
        MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();
        store.putMonitoredFeed("test-feed1");
        store.putMonitoredFeed("test-feed2");
        Assert.assertNull(store.getLastInstanceTime("test-feed1"));

        Date dateOne =  SchemaHelper.parseDateUTC("2015-11-20T00:00Z");
        Date dateTwo =  SchemaHelper.parseDateUTC("2015-11-20T01:00Z");

        store.putPendingInstances("test-feed1", "test_cluster", dateTwo);
        store.putPendingInstances("test-feed1", "test_cluster", dateOne);
        store.putPendingInstances("test-feed2", "test_cluster", dateOne);

        Assert.assertTrue(dateTwo.equals(store.getLastInstanceTime("test-feed1")));
        Assert.assertTrue(dateOne.equals(store.getLastInstanceTime("test-feed2")));

    }

    @Test
    public void testputSLALowCandidate() throws Exception{
        MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();
        Date dateOne =  SchemaHelper.parseDateUTC("2015-11-20T00:00Z");
        store.putSLAAlertInstance("test-feed1", "test-cluster", dateOne, Boolean.TRUE, Boolean.FALSE);
        Assert.assertEquals(Boolean.TRUE, store.getFeedAlertInstance("test-feed1",
                "test-cluster", dateOne).getIsSLALowMissed());
        Assert.assertTrue(dateOne.equals(store.getFeedAlertInstance("test-feed1",
                "test-cluster", dateOne).getNominalTime()));
        store.updateSLAAlertInstance("test-feed1", "test-cluster", dateOne);
        Assert.assertEquals(Boolean.TRUE, store.getFeedAlertInstance("test-feed1",
                "test-cluster", dateOne).getIsSLAHighMissed());
    }

    @Test
    public void testupdateSLAHighCandidate() throws Exception{
        MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();
        Date dateOne =  SchemaHelper.parseDateUTC("2015-11-20T00:00Z");

        store.putSLAAlertInstance("test-feed1", "test-cluster", dateOne, Boolean.TRUE, Boolean.FALSE);
        store.updateSLAAlertInstance("test-feed1", "test-cluster", dateOne);
        Assert.assertEquals(Boolean.TRUE, store.getFeedAlertInstance("test-feed1",
                "test-cluster", dateOne).getIsSLAHighMissed());
    }

    private void clear() {
        EntityManager em = FalconJPAService.get().getEntityManager();
        em.getTransaction().begin();
        try {
            Query query = em.createNativeQuery("delete from MONITORED_FEEDS");
            query.executeUpdate();
            query = em.createNativeQuery("delete from PENDING_INSTANCES");
            query.executeUpdate();
            query = em.createNativeQuery("delete from FEED_SLA_ALERTS");
            query.executeUpdate();

        } finally {
            em.getTransaction().commit();
            em.close();
        }
    }
}
