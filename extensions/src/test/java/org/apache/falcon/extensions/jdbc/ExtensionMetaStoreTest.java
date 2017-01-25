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
package org.apache.falcon.extensions.jdbc;

import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.extensions.ExtensionType;
import org.apache.falcon.extensions.store.AbstractTestExtensionStore;
import org.apache.falcon.persistence.ExtensionBean;
import org.apache.falcon.service.FalconJPAService;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;

/**
 * Test Cases for ExtensionMetaStore.
 */


public class ExtensionMetaStoreTest extends AbstractTestExtensionStore {
    protected EmbeddedCluster dfsCluster;
    protected Configuration conf = new Configuration();
    private static ExtensionMetaStore stateStore;

    @BeforeClass
    public void setup() throws Exception {
        initExtensionStore();
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        stateStore = new ExtensionMetaStore();
    }

    @BeforeMethod
    public void init() {
        clear();
    }

    @Test
    public void testExtension() {
        //insert
        stateStore.storeExtensionBean("test1", "test_location", ExtensionType.TRUSTED, "test_description",
                "falconUser");

        Assert.assertEquals(stateStore.getAllExtensions().size(), 1);
        //check data
        ExtensionBean bean = stateStore.getDetail("test1");
        Assert.assertEquals(bean.getLocation(), "test_location");
        //delete
        stateStore.deleteExtensionsOfType(ExtensionType.TRUSTED);
        Assert.assertEquals(stateStore.getAllExtensions().size(), 0);
    }

    @Test
    public void testExtensionJob() {
        stateStore.storeExtensionBean("test2", "test_location", ExtensionType.CUSTOM, "test2_description",
                "falconUser");
        List<String> processes = new ArrayList<>();
        processes.add("testProcess");
        List<String> feeds = new ArrayList<>();
        feeds.add("testFeed");

        byte[] config = new byte[0];
        stateStore.storeExtensionJob("job1", "test2", feeds, processes, config);
        //storing again to check for entity manager merge to let submission go forward.
        stateStore.storeExtensionJob("job1", "test2", feeds, processes, config);

        Assert.assertEquals(stateStore.getJobsForAnExtension("test2").size(), 1);
        Assert.assertEquals(stateStore.getJobsForAnExtension("test2").get(0).getJobName(), "job1");
        Assert.assertEquals(stateStore.getAllExtensionJobs().size(), 1);
        Assert.assertEquals(stateStore.getExtensionJobDetails("job1").getFeeds().get(0), "testFeed");
        stateStore.deleteExtensionJob("job1");
        Assert.assertEquals(stateStore.getAllExtensionJobs().size(), 0);
    }

    private void clear() {
        EntityManager entityManager = FalconJPAService.get().getEntityManager();
        entityManager.getTransaction().begin();
        try {
            Query query = entityManager.createNativeQuery("delete from EXTENSIONS");
            query.executeUpdate();
            query = entityManager.createNativeQuery("delete from EXTENSION_JOBS");
            query.executeUpdate();
        } finally {
            entityManager.getTransaction().commit();
            entityManager.close();
        }
    }
}
