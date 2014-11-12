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

package org.apache.falcon.regression.entity;

import com.google.common.collect.Ordering;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.EntityResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.CleanupUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * Testing the list entities API.
 */
@Test(groups = "embedded")
public class ListEntitiesTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(ListEntitiesTest.class);
    private String testDir = "/ListEntitiesTest";
    private String baseTestHDFSDir = baseHDFSDir + testDir;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String[] tags = {"first=yes", "second=yes", "third=yes", "wrong=no"};
    private static final Comparator<EntityResult> NAME_COMPARATOR = new Comparator<EntityResult>() {
        @Override
        public int compare(EntityResult o1, EntityResult o2) {
            return o1.name.compareTo(o2.name);
        }
    };

    /**
     * Upload 10+ feeds, processes and clusters with different statuses and tags.
     */
    @BeforeClass(alwaysRun = true)
    public void prepareData()
            throws IOException, AuthenticationException, JAXBException, URISyntaxException,
            InterruptedException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        CleanupUtil.cleanAllEntities(prism);

        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitBundle(prism);

        //submit 10 different clusters, feeds and processes
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        ProcessMerlin process = new ProcessMerlin(bundles[0].getProcessData());
        ClusterMerlin cluster = bundles[0].getClusterElement();
        for (int i = 0; i < 10; i++) {
            process.setName("process" + Util.getUniqueString());
            process.setTags(getRandomTags());
            if (i % 2 == 0) {
                AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(process.toString()));
            } else {
                AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));
            }

            feed.setName("feed" + Util.getUniqueString());
            feed.setTags(getRandomTags());
            if (i % 2 == 0) {
                AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feed.toString()));
            } else {
                AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed.toString()));
            }

            cluster.setName("cluster" + Util.getUniqueString());
            cluster.setTags(getRandomTags());
            AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster.toString()));
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException {
        cleanTestDirs();
        CleanupUtil.cleanAllEntities(prism);
    }

    /**
     * Testing orderBy parameter. Entities should be ordered by name when orderBy=name.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithOrderBy(IEntityManagerHelper helper)
            throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        List<EntityResult> entities =
            helper.listAllEntities("orderBy=name", null).getEntitiesResult().getEntities();
        LOGGER.info(helper.getEntityType() + " entities: " + entities);
        Assert.assertTrue(Ordering.from(NAME_COMPARATOR).isOrdered(entities),
            helper.getEntityType() + " entities are not ordered by name: " + entities);
    }


    /**
     * Filter entities by status (SUBMITTED or RUNNING).
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithFilterByStatus(IEntityManagerHelper helper)
            throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String[] statuses = helper.getEntityType().equalsIgnoreCase("cluster")
            ? new String[]{"SUBMITTED"} : new String[]{"SUBMITTED", "RUNNING"};

        List<EntityResult> allEntities =
            helper.listAllEntities("fields=status", null).getEntitiesResult().getEntities();
        int[] counters = new int[statuses.length];

        for (EntityResult entity : allEntities) {
            for (int i = 0; i < statuses.length; i++) {
                if (statuses[i].equals(entity.status)) {
                    counters[i]++;
                }
            }
        }

        for (int i = 0; i < statuses.length; i++) {
            List<EntityResult> entities = helper.listAllEntities("fields=status&filterBy=STATUS:"
                + statuses[i], null).getEntitiesResult().getEntities();
            Assert.assertEquals(entities.size(), counters[i],
                "Number of entities is not correct with status=" + statuses[i]);
            for (EntityResult entity : entities) {
                Assert.assertEquals(entity.status, statuses[i], "Entity should has status "
                    + statuses[i] + ". Entity: " + entity);
            }

        }
    }

    /**
     * Testing offset parameter. Checking number of entities and order.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithOffset(IEntityManagerHelper helper)
            throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        List<EntityResult> allEntities =
            helper.listAllEntities(null, null).getEntitiesResult().getEntities();
        LOGGER.info(helper.getEntityType() + " entities: " + allEntities);
        int allEntitiesCount = allEntities.size();
        for (int i = 0; i <= allEntitiesCount; i++) {

            List<EntityResult> entities =
                helper.listEntities("offset=" + i, null).getEntitiesResult().getEntities();
            LOGGER.info(String.format("%s entities with offset %d: %s",
                helper.getEntityType(), i, entities));

            Assert.assertEquals(entities.size(),
                allEntitiesCount - i < 10 ? allEntitiesCount - i : 10,
                "Number of entities is not correct.");
            for (int j = 0; j < entities.size(); j++) {
                Assert.assertEquals(entities.get(j).name, allEntities.get(j + i).name,
                    "Order of entities is not correct");
            }
        }
    }

    /**
     * Testing numResults parameter. Checking number of entities and order.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithNumResults(IEntityManagerHelper helper)
            throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        List<EntityResult> allEntities =
            helper.listAllEntities(null, null).getEntitiesResult().getEntities();
        int allEntitiesCount = allEntities.size();

        for (int i = 1; i <= allEntitiesCount; i++) {
            List<EntityResult> entities =
                helper.listEntities("numResults=" + i, null).getEntitiesResult().getEntities();
            Assert.assertEquals(entities.size(), i,
                "Number of entities is not equal to numResults parameter");
            for (int j = 0; j < i; j++) {
                Assert.assertEquals(entities.get(j).name, allEntities.get(j).name,
                    "Order of entities is not correct");
            }
        }

    }

    /**
     * Get list of entities with tag.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithTags(IEntityManagerHelper helper)
            throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        List<EntityResult> allEntities =
            helper.listAllEntities("fields=tags", null).getEntitiesResult().getEntities();
        int[] counters = new int[tags.length];

        for (EntityResult entity : allEntities) {
            for (int i = 0; i < tags.length; i++) {
                if (entity.tag != null && entity.tag.contains(tags[i])) {
                    counters[i]++;
                }
            }
        }

        for (int i = 0; i < tags.length; i++) {
            List<EntityResult> entities = helper.listAllEntities("fields=tags&tags=" + tags[i],
                null).getEntitiesResult().getEntities();
            Assert.assertEquals(entities.size(), counters[i],
                "Number of entities is not correct with tag=" + tags[i]);
            for (EntityResult entity : entities) {
                Assert.assertNotNull(entity.tag, "Entity should have tags. Entity: " + entity);
                Assert.assertTrue(entity.tag.contains(tags[i]), "Entity should contain tag "
                    + tags[i] + ". Entity: " + entity);
            }

        }

    }

    /**
     * Testing list entities API with custom filter.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithCustomFilter(IEntityManagerHelper helper)
            throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        List<EntityResult> entities = helper.listEntities(
            "numResults=2&fields=status,tags&filterBy=STATUS:SUBMITTED&orderBy=name&tags=" + tags[2],
            null).getEntitiesResult().getEntities();
        SoftAssert softAssert = new SoftAssert();
        for (EntityResult entity : entities) {
            softAssert.assertEquals(entity.status, "SUBMITTED",
                "Entities should have status 'SUBMITTED'");
            softAssert.assertTrue(entity.tag.contains(tags[2]), "There is entity without tag="
                + tags[2] + " Entity: " + entity);
        }
        softAssert.assertTrue(entities.size() <= 3, "Number of results should be 3 or less");
        softAssert.assertTrue(Ordering.from(NAME_COMPARATOR).isOrdered(entities),
            helper.getEntityType() + " entities are not ordered by name: " + entities);
        softAssert.assertAll();
    }


    @DataProvider
    public Object[][] getHelpers(){
        return new Object[][]{
            {prism.getProcessHelper()},
            {prism.getClusterHelper()},
            {prism.getFeedHelper()},
        };
    }

    private String getRandomTags() {
        List<String> tagsList = new ArrayList<String>();
        Random r = new Random();
        if (r.nextInt(4) == 0) {
            tagsList.add(tags[0]);
        }
        if (r.nextInt(3) == 0) {
            tagsList.add(tags[1]);
        }
        if (r.nextInt(2) == 0) {
            tagsList.add(tags[2]);
        }
        return StringUtils.join(tagsList, ',');
    }
}
