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

package org.apache.falcon.regression.lineage;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.EntityList.EntityElement;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Submitting and listing processes with different pipeline names.
 */
@Test(groups = "embedded")
public class ProcessPipelineTest extends BaseTestClass{
    private static final Logger LOGGER = Logger.getLogger(ProcessPipelineTest.class);
    private ColoHelper cluster = servers.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void setUp() throws IOException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void prepareData() throws IOException {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(){
        removeTestClassEntities();
    }

    /**
     * Submit List of processes with a given pipeline tag. There are few pipelines and for each
     * pipeline there is different number of processes. Test expects an appropriate list of
     * processes for each given pipeline tag.
     */
    @Test
    public void listPipeline()
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        InterruptedException {
        //match processes to pipelines
        HashMap<String, List<String>> map = new HashMap<>();
        //index for few different pipelines
        for(int p = 0, i = 0, n = 0, d = 3; p < 3; p++, d++){
            n += d + 1;
            String pipeline = "pipeline" + p;
            map.put(pipeline, new ArrayList<String>());
            //index for new processes for current pipeline
            for(; i < n; i++){
                String processName = Util.getEntityPrefix(this) + "-process-" + i;
                bundles[0].setProcessName(processName);
                bundles[0].setProcessPipeline(pipeline);
                bundles[0].submitProcess(true);
                map.get(pipeline).add(processName);
            }
        }
        LOGGER.info("Expected set of processes: " + map);
        //now go through pipelines and check that their processes lists match to expected
        for(int p = 0, n = 3; p < 3; p++, n++){
            String pipeline = "pipeline" + p;
            ServiceResponse response = prism.getProcessHelper().getListByPipeline(pipeline);
            EntityElement[] processes = response.getEntityList().getElements();
            //check that all retrieved processes match to expected list
            List<String> expected = map.get(pipeline);
            Assert.assertEquals(processes.length, expected.size(),
                String.format("Invalid number of processes for pipeline [%s].", pipeline));
            for(EntityElement process : processes){
                Assert.assertTrue(expected.contains(process.name), String.format("Expected "
                    + "list %s doesn't contain %s for %s.", expected, process.name, pipeline));
            }
        }
    }

    /**
     * Submit a process with pipeline element, get process definition expecting retrieved xml to
     * be the same.
     */
    @Test
    public void testProcessWithPipeline()
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        SAXException, InterruptedException {
        String pipeline = "samplePipeline";
        bundles[0].setProcessPipeline(pipeline);
        bundles[0].submitProcess(true);
        String process = bundles[0].getProcessData();
        String processDef = prism.getProcessHelper().getEntityDefinition(process).getMessage();
        Assert.assertTrue(XmlUtil.isIdentical(process, processDef), "Definitions are not equal.");
    }

    /**
     * Test cases:
     * -list processes with pipeline name having special or UTF-8 chars expecting request to fail.
     * -list processes with invalid or long pipeline name expecting request to pass.
     * -submit a process with pipeline name with special or UTF-8 chars expecting submission to fail.
     * -submit a process with a long pipeline name or many comma separated names process submission
     * to go through.
     *
     * @param pipeline pipeline name
     * @param action list or submit entities
     * @param shouldSucceed should action succeed or not
     */
    @Test(dataProvider = "data")
    public void testPipelines(String pipeline, String action, boolean shouldSucceed)
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        InterruptedException {
        bundles[0].setProcessPipeline(pipeline);
        if (action.equals("list")){
            if (shouldSucceed){
                AssertUtil.assertSucceeded(cluster.getProcessHelper().getListByPipeline(pipeline));
            }else {
                AssertUtil.assertFailed(cluster.getProcessHelper().getListByPipeline(pipeline));
            }
        } else {
            bundles[0].submitProcess(shouldSucceed);
        }
    }

    @DataProvider(name = "data")
    public Object[][] getTestData(){
        String specialName = "$pec!alN@me#1";
        String utf8Name = "UTF8Pipelin" + "\u20AC"; //euro symbol added to name
        String longName = "TestItWithPipelineNameWhichIsLongEnough";
        return new Object[][]{//{specialName, "list", false},
            //{utf8Name, "list", false},
            {"nonexistentPipeline", "list", true}, //expecting blank response
            {longName, "list", true}, //expecting blank response
            //{specialName, "submit", false}, {utf8Name, "submit", false},
            {longName, "submit", true},
            {"pipeline0,pipeline1,pipeline2,pipeline3,pipeline4,pipeline5,pipeline6,pipeline7,"
                 + "pipeline8,pipeline9,pipeline10,pipeline11", "submit", true,
            },
        };
    }
}
