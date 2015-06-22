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
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.EntityList.EntityElement;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * Testing the list entities API.
 */
@Test(groups = "embedded")
public class ListEntitiesTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(ListEntitiesTest.class);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String[] tags = {"first=yes", "second=yes", "third=yes", "wrong=no"};
    private static final Comparator<EntityElement> NAME_COMPARATOR = new Comparator<EntityElement>() {
        @Override
        public int compare(EntityElement o1, EntityElement o2) {
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
        removeTestClassEntities();

        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitBundle(prism);

        //submit 10 different clusters, feeds and processes
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        ProcessMerlin process = bundles[0].getProcessObject();
        ClusterMerlin cluster = bundles[0].getClusterElement();
        String clusterNamePrefix = bundles[0].getClusterElement().getName() + '-';
        String processNamePrefix = bundles[0].getProcessName() + '-';
        String feedNamePrefix = bundles[0].getInputFeedNameFromBundle() + '-';
        for (int i = 0; i < 10; i++) {
            process.setName(processNamePrefix + i);
            process.setTags(getRandomTags());
            if (i % 2 == 0) {
                AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(process.toString()));
            } else {
                AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));
            }

            feed.setName(feedNamePrefix + i);
            feed.setTags(getRandomTags());
            if (i % 2 == 0) {
                AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feed.toString()));
            } else {
                AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed.toString()));
            }

            cluster.setName(clusterNamePrefix + i);
            cluster.setTags(getRandomTags());
            AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster.toString()));
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
    }

    /**
     * Testing orderBy parameter. Entities should be ordered by name when orderBy=name.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithOrderBy(AbstractEntityHelper helper)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        EntityElement[] entities =
            helper.listAllEntities("orderBy=name", null).getEntityList().getElements();
        LOGGER.info(helper.getEntityType() + " entities: " + Arrays.toString(entities));
        Assert.assertTrue(Ordering.from(NAME_COMPARATOR).isOrdered(Arrays.asList(entities)),
            helper.getEntityType() + " entities are not ordered by name: " + Arrays.toString(entities));
    }


    /**
     * Filter entities by status (SUBMITTED or RUNNING).
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithFilterByStatus(AbstractEntityHelper helper)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        String[] statuses = helper.getEntityType().equalsIgnoreCase("cluster")
            ? new String[]{"SUBMITTED"} : new String[]{"SUBMITTED", "RUNNING"};

        EntityElement[] allEntities =
            helper.listAllEntities("fields=status", null).getEntityList().getElements();
        int[] counters = new int[statuses.length];

        for (EntityElement entity : allEntities) {
            for (int i = 0; i < statuses.length; i++) {
                if (statuses[i].equals(entity.status)) {
                    counters[i]++;
                }
            }
        }

        for (int i = 0; i < statuses.length; i++) {
            EntityElement[] entities = helper.listAllEntities("fields=status&filterBy=STATUS:"
                + statuses[i], null).getEntityList().getElements();
            Assert.assertEquals(entities.length, counters[i],
                "Number of entities is not correct with status=" + statuses[i]);
            for (EntityElement entity : entities) {
                Assert.assertEquals(entity.status, statuses[i], "Entity should has status "
                    + statuses[i] + ". Entity: " + entity);
            }

        }
    }

    /**
     * Testing offset parameter. Checking number of entities and order.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithOffset(AbstractEntityHelper helper)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        EntityElement[] allEntities =
            helper.listAllEntities().getEntityList().getElements();
        LOGGER.info(helper.getEntityType() + " entities: " + Arrays.toString(allEntities));
        int allEntitiesCount = allEntities.length;
        for (int i = 0; i <= allEntitiesCount; i++) {

            EntityElement[] entities =
                helper.listEntities(null, "offset=" + i, null).getEntityList().getElements();
            LOGGER.info(String.format("%s entities with offset %d: %s",
                helper.getEntityType(), i, Arrays.toString(entities)));

            Assert.assertEquals(entities != null ? entities.length : 0,
                allEntitiesCount - i < 10 ? allEntitiesCount - i : 10,
                "Number of entities is not correct.");

            for (int j = 0; entities != null && j < entities.length; j++) {
                Assert.assertEquals(entities[j].name, allEntities[j + i].name,
                    "Order of entities is not correct");
            }
        }
    }

    /**
     * Testing numResults parameter. Checking number of entities and order.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithNumResults(AbstractEntityHelper helper)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        EntityElement[] allEntities =
            helper.listAllEntities().getEntityList().getElements();
        int allEntitiesCount = allEntities.length;

        for (int i = 1; i <= allEntitiesCount; i++) {
            EntityElement[] entities =
                helper.listEntities(null, "numResults=" + i, null).getEntityList().getElements();
            Assert.assertEquals(entities.length, i,
                "Number of entities is not equal to numResults parameter");
            for (int j = 0; j < i; j++) {
                Assert.assertEquals(entities[j].name, allEntities[j].name,
                    "Order of entities is not correct");
            }
        }

    }

    /**
     * Get list of entities with tag.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithTags(AbstractEntityHelper helper)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        EntityElement[] allEntities =
            helper.listAllEntities("fields=tags", null).getEntityList().getElements();
        int[] counters = new int[tags.length];

        for (EntityElement entity : allEntities) {
            for (int i = 0; i < tags.length; i++) {
                if (entity.tag != null && entity.tag.contains(tags[i])) {
                    counters[i]++;
                }
            }
        }

        for (int i = 0; i < tags.length; i++) {
            EntityElement[] entities = helper.listAllEntities("fields=tags&tags=" + tags[i],
                null).getEntityList().getElements();
            Assert.assertEquals(entities != null ? entities.length : 0, counters[i],
                "Number of entities is not correct with tag=" + tags[i]);

            for (int j = 0; entities != null && j < entities.length; j++) {
                Assert.assertNotNull(entities[j].tag, "Entity should have tags. Entity: "
                    + entities[j]);
                Assert.assertTrue(entities[j].tag.contains(tags[i]), "Entity should contain tag "
                    + tags[i] + ". Entity: " + entities[j]);
            }

        }

    }

    /**
     * Testing list entities API with custom filter.
     */
    @Test(dataProvider = "getHelpers")
    public void listEntitiesWithCustomFilter(AbstractEntityHelper helper)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {

        EntityElement[] entities = helper.listEntities(
            null, "numResults=2&fields=status,tags&filterBy=STATUS:SUBMITTED&orderBy=name&tags=" + tags[2],
            null).getEntityList().getElements();
        if (entities != null) {
            SoftAssert softAssert = new SoftAssert();
            for (EntityElement entity : entities) {
                softAssert.assertEquals(entity.status, "SUBMITTED",
                    "Entities should have status 'SUBMITTED'");
                softAssert.assertTrue(entity.tag.contains(tags[2]), "There is entity without tag="
                    + tags[2] + " Entity: " + entity);
            }

            softAssert.assertTrue(entities.length <= 2, "Number of results should be 2 or less");

            softAssert.assertTrue(Ordering.from(NAME_COMPARATOR).isOrdered(Arrays.asList(entities)),
                helper.getEntityType() + " entities are not ordered by name: " + Arrays.toString(entities));
            softAssert.assertAll();
        }
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
        List<String> tagsList = new ArrayList<>();
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
