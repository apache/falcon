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
package org.apache.falcon.regression;

import com.google.common.collect.Ordering;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.EntityList.EntityElement;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Search api tests.
 */
@Test(groups = "search-api")
public class SearchApiTest extends BaseTestClass {

    private static final Logger LOGGER = Logger.getLogger(SearchApiTest.class);
    private ColoHelper cluster = servers.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private final String base = Util.getEntityPrefix(this);
    private static final Comparator<EntityElement> ASC = new Comparator<EntityElement>() {
        @Override
        public int compare(EntityElement o1, EntityElement o2) {
            return o1.name.compareTo(o2.name);
        }
    };
    private static final Comparator<EntityElement> DESC = Collections.reverseOrder(ASC);

    /**
     * Method creates a set of entities.
     * |--------------------------------------------+-----------------------------+-----------------------------|
     * | Basic input/output Feeds (without tags)    | bundle0-input-feed          | bundle0-output-feed         |
     * |--------------+-----------------------------+-----------------------------+-----------------------------|
     * | Feed Name    | bundle1-feed                | bundle2-feed                | bundle3-feed                |
     * | Tags         | specific=bundle1            | specific=bundle2            | specific=bundle3            |
     * |              | common=common               | common=common               | common=common               |
     * |              | partial=b1b2                | partial=b1b2                |                             |
     * |--------------+-----------------------------+-----------------------------+-----------------------------|
     * | Process Name | bundle1-process             | bundle2-process             | bundle3-process             |
     * | Tags         | specific=bundle1            | specific=bundle2            | specific=bundle3            |
     * |              | partial=b1b2                | partial=b1b2                | common=common               |
     * |              | common=common               | common=common               |                             |
     * |--------------+-----------------------------+-----------------------------+-----------------------------|
     * | Mirror Name  | bundle1-mirror-process      | bundle2-mirror-process      | bundle3-mirror-process      |
     * | Tags         | specific=bundle1            | specific=bundle2            | specific=bundle3            |
     * |              | common=common               | common=common               | common=common               |
     * |              | partial=b1b2                | partial=b1b2                | _falcon_mirroring_type=HDFS |
     * |              | _falcon_mirroring_type=HDFS | _falcon_mirroring_type=HIVE |                             |
     * |--------------+-----------------------------+-----------------------------+-----------------------------|
     */
    @BeforeClass(alwaysRun = true)
    public void prepareData()
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException, JAXBException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        /* Prepare bundle template*/
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(cluster);
        String prefix = base + "-bundle";

        FeedMerlin basicFeed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        basicFeed.setName(prefix + "0-input-feed");
        AssertUtil.assertSucceeded(cluster.getFeedHelper().submitAndSchedule(basicFeed.toString()));
        basicFeed = new FeedMerlin(bundles[0].getOutputFeedFromBundle());
        basicFeed.setName(prefix + "0-output-feed");
        AssertUtil.assertSucceeded(cluster.getFeedHelper().submitAndSchedule(basicFeed.toString()));

        /* Submit 3 bundles of feeds */
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        for (int i = 1; i <= 3; i++) {
            String feedName = prefix + i + "-feed";
            feed.setName(feedName);
            String tags = "specific=bundle" + i + ",common=common";
            if (i <= 2) {
                tags += ",partial=b1b2";
            }
            feed.setTags(tags);
            AssertUtil.assertSucceeded(cluster.getFeedHelper().submitEntity(feed.toString()));
        }

        /* Submit 3 bundles of processes */
        ProcessMerlin process = bundles[0].getProcessObject();

        //replace input and output with feeds of bundle0
        process.addInputFeed("input", prefix + "0-input-feed");
        process.getInputs().getInputs().remove(0);
        process.addOutputFeed("output", prefix + "0-output-feed");
        process.getOutputs().getOutputs().remove(0);
        process.setWorkflow(aggregateWorkflowDir, null, null);
        for (int i = 1; i <= 3; i++) {
            process.setName(prefix + i + "-process");
            String tags = "specific=bundle" + i + ",common=common";
            if (i <= 2) {
                tags += ",partial=b1b2";
            }
            process.setTags(tags);
            AssertUtil.assertSucceeded(cluster.getProcessHelper().submitEntity(process.toString()));

            //submit a mirroring process
            if (i % 2 == 1) {
                tags += ",_falcon_mirroring_type=HDFS";
            } else {
                tags += ",_falcon_mirroring_type=HIVE";
            }
            process.setName(prefix + i + "-mirror-process");
            process.setTags(tags);
            AssertUtil.assertSucceeded(cluster.getProcessHelper().submitEntity(process.toString()));
        }
    }

    /**
     * Test hits API, retrieves entities and validates them based on set of expected conditions.
     * @param params object which consists of test case parameters such as type, name sequence, tag keys etc.
     * @param result object which contains set of expected conditions. Performs validations of entities.
     */
    @Test(dataProvider = "getSearchParams")
    public void search(QueryParams params, Result result)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException, JAXBException {
        ServiceResponse serviceResponse = cluster.getClusterHelper()
            .listEntities(params.getType(), params.getParams(), null);
        if (result.getExpError() != null) {
            AssertUtil.assertFailed(serviceResponse, result.getExpError());
        } else {
            String order = null;
            if (params.getParams().contains("orderBy")) {
                order = params.getParams().contains("sortOrder=desc") ? "desc" : "asc";
            }
            result.validateEntities(serviceResponse.getEntityList().getElements(), order);
        }
    }

    @DataProvider
    private Object[][] getSearchParams() {
        return new Object[][]{

            /*Nameseq test cases*/
            {new QueryParams().withNumResults(12), new Result().withExpBundles(0, 1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(11), },
            {new QueryParams().withNameSeq(base + "-bundle1-feed"), new Result().withExpBundles(1)
                .withExpTypes("feed").withExpTotal(1), },
            {new QueryParams().withNameSeq(base + "-bundle2-mirror-process"), new Result().withExpBundles(2)
                .withExpTypes("process", "mirror").withExpTotal(1), },
            {new QueryParams().withNameSeq(base + "-bUnDlE1-fEeD"), new Result().withExpBundles(1)
                .withExpTypes("feed").withExpTotal(1), },
            {new QueryParams().withNameSeq(base + "-bundle1-feed-non"), new Result(), },
            {new QueryParams().withNameSeq(base + "-bundle2-process-non"), new Result(), },
            {new QueryParams().withNameSeq("-bundle2-"), new Result().withExpBundles(2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withNameSeq(base).withNumResults(12), new Result().withExpBundles(0, 1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(11), },
            {new QueryParams().withNameSeq("bundleFeed"), new Result().withExpBundles(0, 1, 2, 3)
                .withExpTypes("feed").withExpTotal(5), },
            {new QueryParams().withNameSeq("bundleProcess"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("process", "mirror").withExpTotal(6), },
            {new QueryParams().withNameSeq("bUnDlEfeEd"), new Result().withExpBundles(0, 1, 2, 3)
                .withExpTypes("feed").withExpTotal(5), },
            {new QueryParams().withNameSeq("bUnDlEprOCesS"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("process", "mirror").withExpTotal(6), },
            {new QueryParams().withNameSeq(base + "-bundle-nonexistent"), new Result(), },
            //unusual nameseq forms
            {new QueryParams().withNameSeq("012345"), new Result(), },
            {new QueryParams().withNameSeq("f€€d"), new Result(), },
            {new QueryParams().withNameSeq("_-#$@"), new Result(), },
            {new QueryParams().withNameSeq("").withNumResults(12), new Result().withExpBundles(0, 1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(11), },

            /*Tagkey test cases*/
            /* Full tagkey name*/
            {new QueryParams().withTagkeys("bundle1"), new Result().withExpBundles(1)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("bundle2"), new Result().withExpBundles(2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("bundle3"), new Result().withExpBundles(3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("bUnDlE1"), new Result().withExpBundles(1)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("_falcon_mirroring_type"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("process", "mirror").withExpTotal(3), },
            /* Special, utf-8 symbols*/
            {new QueryParams().withTagkeys("bun@le#"), new Result()},
            {new QueryParams().withTagkeys("bundl€"), new Result()},
            /* Common for a pair of bundles; use both tag and value as tagkey*/
            {new QueryParams().withTagkeys("b1b2"), new Result().withExpBundles(1, 2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(6), },
            {new QueryParams().withTagkeys("partial"), new Result().withExpBundles(1, 2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(6), },
            /* Common for 1,2,3 bundles*/
            {new QueryParams().withTagkeys("common"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(9), },
            {new QueryParams().withTagkeys("bundle"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(9), },
            {new QueryParams().withTagkeys("undle"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(9), },
            {new QueryParams().withTagkeys("undle,undle"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(9), },
            /* Multiple full tags*/
            {new QueryParams().withTagkeys("common,bundle1"), new Result().withExpBundles(1)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("common,bundle2"), new Result().withExpBundles(2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("common,bundle3"), new Result().withExpBundles(3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("common,bundle1,b1b2"), new Result().withExpBundles(1)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("common,bundle2,b1b2"), new Result().withExpBundles(2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            /* Multiple, diff case*/
            {new QueryParams().withTagkeys("cOmMoN,bUnDlE2,B1b2"), new Result().withExpBundles(2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            /* Multiple partial tags*/
            {new QueryParams().withTagkeys("common,undle"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(9), },
            {new QueryParams().withTagkeys("ommo,undle"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(9), },
            {new QueryParams().withTagkeys("ommo,ndle1"), new Result().withExpBundles(1)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("oMMon,ndle2"), new Result().withExpBundles(2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            {new QueryParams().withTagkeys("oMMon,b1b"), new Result().withExpBundles(1, 2)
                .withExpTypes("feed", "process", "mirror").withExpTotal(6), },
            {new QueryParams().withTagkeys("comm,ndle3"), new Result().withExpBundles(3)
                .withExpTypes("feed", "process", "mirror").withExpTotal(3), },
            /* Non existent*/
            {new QueryParams().withTagkeys("bundle9"), new Result(), },
            {new QueryParams().withTagkeys("common,undle,zero"), new Result(), },

            /*Custom filter test cases*/
            //different types
            {new QueryParams().forType("process"), new Result().withExpBundles(1, 2, 3)
                .withExpTypes("process", "mirror").withExpTotal(6), },
            {new QueryParams().forType("feed"), new Result().withExpBundles(0, 1, 2, 3)
                .withExpTypes("feed").withExpTotal(5), },
            {new QueryParams().forType("FEED"), new Result().withExpBundles(0, 1, 2, 3)
                .withExpTypes("feed").withExpTotal(5), },
            {new QueryParams().forType("MEGAFEED"), new Result()
                .withExpError("Invalid entity type: MEGAFEED. Expected [feed, process, cluster]"), },
            //custom filters
            {new QueryParams().forType("process").withNameSeq("bundle").withTagkeys("bundle,b1b2").withNumResults(3)
                .withOrder(true).withSortOrder("desc"), new Result().withExpBundles(1, 2)
                    .withExpTypes("process", "mirror").withExpTotal(3), },
            {new QueryParams().forType("feed,process").withNameSeq("bundle").withTagkeys("partial,common")
                .withOrder(true), new Result().withExpBundles(1, 2).withExpTypes("feed", "process", "mirror")
                .withExpTotal(6), },
            {new QueryParams().forType("feed").withNameSeq("bundlefeed").withNumResults(2).withOrder(true)
                .withSortOrder("asc"), new Result().withExpBundles(0, 1, 2, 3)
                    .withExpTypes("feed").withExpTotal(2), },
            {new QueryParams().forType("feed,process").withNameSeq("bundleproc").withTagkeys("_falcon_mirroring_type")
                .withOrder(true).withSortOrder("desc"), new Result().withExpBundles(1, 2, 3)
                    .withExpTypes("process", "mirror").withExpTotal(3), },
            //one option excludes another
            {new QueryParams().forType("process").withNameSeq("bundlefeed"), new Result(), },
            {new QueryParams().forType("feed").withNameSeq("bundleprocess"), new Result(), },
            {new QueryParams().forType("feed,process").withNameSeq("bundle3").withTagkeys("b1b2").withOrder(true),
                new Result(), },
        };
    }

    /**
     * Contains all required params for search api request.
     */
    private class QueryParams {
        private String type = "feed,process";
        private String nameSeq = null;
        private String tagKeys = null;
        private String orderBy = null;
        private String sortOrder = null;
        private Integer numResults = null;
        private String fields = "&fields=tags";

        public QueryParams forType(String paramType) {
            this.type = paramType;
            return this;
        }

        public QueryParams withNameSeq(String paramNameSeq) {
            this.nameSeq = paramNameSeq;
            return this;
        }

        public QueryParams withTagkeys(String paramTagkey) {
            this.tagKeys = paramTagkey;
            return this;
        }

        public QueryParams withOrder(boolean ordered) {
            if (ordered) {
                this.orderBy = "&orderBy=name";
            }
            return this;
        }

        public QueryParams withSortOrder(String paramSortOrder) {
            this.sortOrder = paramSortOrder;
            return this;
        }

        public QueryParams withNumResults(Integer paramNumResults) {
            this.numResults = paramNumResults;
            return this;
        }

        /**
         * @return url param string.
         */
        public String getParams() {
            return (nameSeq != null ? "&nameseq=" + nameSeq : "")
                + (tagKeys != null ? "&tagkeys=" + tagKeys : "")
                + (numResults != null ? "&numResults=" + numResults : "")
                + (orderBy != null ? "&orderBy=name" + orderBy : "")
                + (sortOrder != null ? "&sortOrder=" + sortOrder : "") + fields;
        }

        public String getType() {
            return this.type;
        }

        /**
         * Pretty prints params to param string.
         */
        @Override
        public String toString() {
            return "[type: " + type + (nameSeq != null ? "; nseq: " + nameSeq : "")
                + (tagKeys != null ? "; tagkeys: " + tagKeys : "")
                + (numResults != null ? "; n:" + numResults : "")
                + (orderBy != null ? "; ordered" : "")
                + (sortOrder != null ? "; " + sortOrder : "") + "]";
        }
    }

    /**
     * Class for entities response validation.
     */
    private class Result {
        private List<String> expectedTypes = null;
        private String expectedError = null;
        private List<String> expectedBundles = null;
        private int expectedTotal = 0;
        private final List<String> allBundles = Arrays.asList("bundle0", "bundle1", "bundle2", "bundle3");
        private final List<String> allTypes =  Arrays.asList("feed", "process", "mirror");

        public Result withExpBundles(int... bundleNums) {
            expectedBundles = new ArrayList<>();
            for (int bundleNum : bundleNums) {
                expectedBundles.add("bundle" + bundleNum);
            }
            return this;
        }

        public Result withExpTypes(String... types) {
            expectedTypes = Arrays.asList(types);
            return this;
        }

        public Result withExpError(String message) {
            this.expectedError = message;
            return this;
        }

        public Result withExpTotal(int total) {
            this.expectedTotal = total;
            return this;
        }

        public String getExpError() {
            return expectedError;
        }

        /**
         * Pretty prints params to param string.
         */
        @Override
        public String toString() {
            if (expectedError != null) {
                return "Expected: error.";
            } else {
                return String.format("[Expected: %d %s%s]", expectedTotal,
                    (expectedTypes != null ? expectedTypes + "s": "entities"),
                    (expectedBundles != null ? " of " + expectedBundles : ""));
            }
        }

        /**
         * Validates entities number and order. Checks that each entity belongs to expected bundle and type.
         * @param entities entities
         * @param order expected order
         */
        public void validateEntities(EntityElement[] entities, String order) {
            //validate number of entities
            if (expectedTotal == 0) {
                assertNull(entities, "Response shouldn't contain entities.");
            } else {
                List<EntityElement> entitiesList = new ArrayList<>(Arrays.asList(entities));
                cleanUpResult(entitiesList);
                LOGGER.info("Entities after clean up: \n" + entitiesList);
                assertEquals(entitiesList.size(), expectedTotal, "Number of entities is not as expected.");

                //Check that each entity belongs to expected bundle and type
                for (EntityElement entity : entitiesList) {
                    assertTrue(matches(entity.name, allBundles, expectedBundles),
                        entity.name + " doesn't belong to expected bundles: " + expectedBundles);
                    assertTrue(matches(entity.name, allTypes, expectedTypes),
                        entity.name + " doesn't belong to expected types: " + expectedTypes);
                }
                //check the order
                if (order != null) {
                    Assert.assertTrue(Ordering.from(order.equals("desc") ? DESC : ASC).isOrdered(entitiesList),
                        String.format("Entities are not ordered in %sending order", order));
                }
            }
        }

        /**
         * Checks that entity name corresponds to expected bundle and type.
         */
        private boolean matches(String name, List<String> all, List<String> expectedParts) {
            boolean matches = false;
            for (String expectedPart : expectedParts) {
                if (name.contains(expectedPart)) {
                    matches = true;
                    break;
                }
            }
            List<String> unexpectedParts = new ArrayList<>(all);
            unexpectedParts.removeAll(expectedParts);
            for (String unexpectedPart : unexpectedParts) {
                if (name.contains(unexpectedPart)) {
                    matches = false;
                    break;
                }
            }
            return matches;
        }
    }

    /**
     * Cleans up a result list from items which belong to everything else than current test class.
     * @param entityElements result array
     */
    private void cleanUpResult(List<EntityElement> entityElements) {
        for (ListIterator<EntityElement> iterator = entityElements.listIterator(); iterator.hasNext();) {
            String name = iterator.next().name;
            if (!name.contains(base)) {
                iterator.remove();
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }
}
