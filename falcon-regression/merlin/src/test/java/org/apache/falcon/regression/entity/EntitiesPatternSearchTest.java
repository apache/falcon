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

import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
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

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Testing the pattern search of entities. Falcon-914
 */
@Test(groups = "embedded")
public class EntitiesPatternSearchTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(EntitiesPatternSearchTest.class);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";

    /**
     * Upload feeds, processes and clusters with different names.
     */
    @BeforeClass(alwaysRun = true)
    public void prepareData()
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException, JAXBException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        removeTestClassEntities();

        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitBundle(prism);

        //submit different clusters, feeds and processes
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        ProcessMerlin process = bundles[0].getProcessObject();
        ClusterMerlin cluster = bundles[0].getClusterElement();
        String clusterNamePrefix = bundles[0].getClusterElement().getName() + '-';
        String processNamePrefix = bundles[0].getProcessName() + '-';
        String feedNamePrefix = bundles[0].getInputFeedNameFromBundle() + '-';
        List randomNames = getPatternName();
        for (Object randomName : randomNames) {
            process.setName(processNamePrefix + randomName);
            AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));

            feed.setName(feedNamePrefix + randomName);
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed.toString()));

            cluster.setName(clusterNamePrefix + randomName);
            AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster.toString()));
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
    }

    /**
     * Testing entity listing for patterns that match and validating the output for the same.
     */
    @Test(dataProvider = "getPattern")
    public void listEntitiesWithPattern(AbstractEntityHelper helper, String patternParam)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        EntityElement[] entities =
                helper.listAllEntities("nameseq=" + patternParam, null).getEntityList().getElements();
        LOGGER.info(helper.getEntityType() + " entities: " + Arrays.toString(entities));
        validateOutputPatternList(helper.listEntities().getEntityList().getElements(), entities, patternParam);
    }

    /**
     * Testing entity listing for patterns that do not match and validating the output for the same.
     */
    @Test(dataProvider = "getMismatchPattern")
    public void listEntitiesWithPatternMismatch(AbstractEntityHelper helper, String mismatchPatternParam)
        throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        mismatchPatternParam = mismatchPatternParam.replaceAll(" ", "%20");
        EntityElement[] entities =
                helper.listAllEntities("nameseq=" + mismatchPatternParam, null).getEntityList().getElements();
        LOGGER.info(helper.getEntityType() + " entities: " + Arrays.toString(entities));
        Assert.assertNull(entities, "No pattern matches");
    }

    @DataProvider
    public Object[][] getPattern() {
        String[] patternParam =
                new String[] {
                    "new", "defintion", "W-E", "NewEntityDefinition", "ned",
                    "NED", "NeD", "N-e-D", "N-e-D123", "", };
        AbstractEntityHelper[] helper = new AbstractEntityHelper[] {
                prism.getProcessHelper(),
                prism.getFeedHelper(),
                prism.getClusterHelper(),
        };
        return MatrixUtil.crossProduct(helper, patternParam);
    }

    @DataProvider
    public Object[][] getMismatchPattern(){
        String[] mismatchPatternParam = new String[]{"akm", "ne d", "newss", "new*", "NewEntityDefinitions"};
        AbstractEntityHelper[] helper = new AbstractEntityHelper[] {
                prism.getProcessHelper(),
                prism.getFeedHelper(),
                prism.getClusterHelper(),
        };
        return MatrixUtil.crossProduct(helper, mismatchPatternParam);
    }

    private void validateOutputPatternList(EntityElement[] entityElements,
            EntityElement[] outputelements, String pattern) {
        List<String> actualOutputElements = new ArrayList<>();
        List<String> expectedOutputElements = new ArrayList<>();
        for(EntityElement e : entityElements) {
            if (getOutputEntity(e.name, pattern)) {
                expectedOutputElements.add(e.name);
            }
        }

        for(EntityElement e : outputelements) {
            actualOutputElements.add(e.name);
        }

        LOGGER.debug("actualElement : " + actualOutputElements);
        LOGGER.debug("expectedElement : " + expectedOutputElements);

        // Checking no of elements present in output.
        AssertUtil.checkForListSizes(expectedOutputElements, actualOutputElements);

        //Checking expected out and actual output contains same enitities.
        Assert.assertTrue(expectedOutputElements.containsAll(actualOutputElements),
                "Output list elements are not as expected");

        for(String element : expectedOutputElements) {
            Assert.assertTrue(actualOutputElements.contains(element),
                    "Element " + element + "is not present in output");
        }
    }

    private Boolean getOutputEntity(String entityName, String pattern) {
        String patternCheck="";
        String regexString=".*";
        StringBuffer newString = new StringBuffer();
        char[] searchPatterns = pattern.toLowerCase().toCharArray();
        for (char searchPattern : searchPatterns) {
            newString = newString.append(regexString).append(searchPattern);
        }
        patternCheck = newString.append(regexString).toString();
        LOGGER.info("patternCheck : " + patternCheck);
        return entityName.toLowerCase().matches(patternCheck);
    }

    private List<String> getPatternName() {
        List<String> nameList = new ArrayList<>();
        nameList.add("New-Entity-Definition");
        nameList.add("NewEntityDefinition");
        nameList.add("newentitydefine");
        nameList.add("NEW-ENTITY");
        nameList.add("NEW1-ENTITY2-DEFINITION123");
        nameList.add("New-definition123");
        return nameList;
    }
}
