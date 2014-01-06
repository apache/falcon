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

package org.apache.falcon.entity.parser;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for validating process entity parser.
 */
public class ProcessEntityParserTest extends AbstractTestBase {

    private final ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory.getParser(EntityType.PROCESS);

    @Test
    public void testNotNullgetUnmarshaller() throws Exception {
        Unmarshaller unmarshaller = EntityType.PROCESS.getUnmarshaller();
        Assert.assertNotNull(unmarshaller);
    }

    @BeforeClass
    public void init() throws Exception {
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
    }

    @AfterClass
    public void tearDown() {
        this.dfsCluster.shutdown();
    }

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.FEED, "impressionFeed");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "imp-click-join1");
        storeEntity(EntityType.FEED, "imp-click-join2");
        storeEntity(EntityType.PROCESS, "sample");
        dfsCluster.getFileSystem().mkdirs(new Path("/falcon/test/workflow"));
    }

    @Test
    public void testParse() throws FalconException, JAXBException {

        Process process = parser.parseAndValidate(getClass().getResourceAsStream(PROCESS_XML));

        Assert.assertNotNull(process);
        Assert.assertEquals(process.getName(), "sample");

        Assert.assertEquals(process.getParallel(), 1);
        Assert.assertEquals(process.getOrder().name(), "LIFO");
        Assert.assertEquals(process.getFrequency().toString(), "hours(1)");
        Assert.assertEquals(process.getEntityType(), EntityType.PROCESS);

        Assert.assertEquals(process.getInputs().getInputs().get(0).getName(), "impression");
        Assert.assertEquals(process.getInputs().getInputs().get(0).getFeed(), "impressionFeed");
        Assert.assertEquals(process.getInputs().getInputs().get(0).getStart(), "today(0,0)");
        Assert.assertEquals(process.getInputs().getInputs().get(0).getEnd(), "today(2,0)");
        Assert.assertEquals(process.getInputs().getInputs().get(0).getPartition(), "*/US");
        Assert.assertEquals(process.getInputs().getInputs().get(0).isOptional(), false);

        Assert.assertEquals(process.getOutputs().getOutputs().get(0).getName(), "impOutput");
        Assert.assertEquals(process.getOutputs().getOutputs().get(0).getFeed(), "imp-click-join1");
        Assert.assertEquals(process.getOutputs().getOutputs().get(0).getInstance(), "today(0,0)");

        Assert.assertEquals(process.getProperties().getProperties().get(0).getName(), "name1");
        Assert.assertEquals(process.getProperties().getProperties().get(0).getValue(), "value1");

        Cluster processCluster = process.getClusters().getClusters().get(0);
        Assert.assertEquals(SchemaHelper.formatDateUTC(processCluster.getValidity().getStart()), "2011-11-02T00:00Z");
        Assert.assertEquals(SchemaHelper.formatDateUTC(processCluster.getValidity().getEnd()), "2011-12-30T00:00Z");
        Assert.assertEquals(process.getTimezone().getID(), "UTC");

        Assert.assertEquals(process.getWorkflow().getEngine().name().toLowerCase(), "oozie");
        Assert.assertEquals(process.getWorkflow().getPath(), "/falcon/test/workflow");

        StringWriter stringWriter = new StringWriter();
        Marshaller marshaller = EntityType.PROCESS.getMarshaller();
        marshaller.marshal(process, stringWriter);
        System.out.println(stringWriter.toString());

        // TODO for retry and late policy
    }

    @Test
    public void testELExpressions() throws Exception {
        Process process = parser.parseAndValidate(getClass().getResourceAsStream(PROCESS_XML));
        process.getInputs().getInputs().get(0).setStart("lastMonth(0,0,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) {
            //ignore
        }

        process.getInputs().getInputs().get(0).setStart("today(0,0)");
        process.getInputs().getInputs().get(0).setEnd("lastMonth(0,0,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) {
            //ignore
        }

        process.getInputs().getInputs().get(0).setStart("today(2,0)");
        process.getInputs().getInputs().get(0).setEnd("today(0,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) {
            //ignore
        }
    }

    @Test(expectedExceptions = FalconException.class)
    public void doParseInvalidXML() throws IOException, FalconException {

        String invalidProcessXml = "/config/process/process-invalid.xml";
        parser.parseAndValidate(this.getClass().getResourceAsStream(invalidProcessXml));
    }

    @Test(expectedExceptions = ValidationException.class)
    public void applyValidationInvalidProcess() throws Exception {
        Process process = parser.parseAndValidate(getClass().getResourceAsStream(PROCESS_XML));
        process.getClusters().getClusters().get(0).setName("invalid cluster");
        parser.validate(process);
    }

    @Test(expectedExceptions = FalconException.class)
    public void testValidate() throws FalconException {
        parser.parseAndValidate("<process></process>");
    }

    //SUSPEND CHECKSTYLE CHECK HiddenFieldCheck
    @Test
    public void testConcurrentParsing() throws Exception {
        List<Thread> threadList = new ArrayList<Thread>();

        for (int i = 0; i < 3; i++) {
            threadList.add(new Thread() {
                @Override
                public void run() {
                    try {
                        EntityParser parser = EntityParserFactory.getParser(EntityType.PROCESS);
                        parser.parseAndValidate(this.getClass().getResourceAsStream(PROCESS_XML));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        for (Thread thread : threadList) {
            thread.start();
        }
        for (Thread thread : threadList) {
            thread.join();
        }
    }
    //RESUME CHECKSTYLE CHECK HiddenFieldCheck

    @Test(expectedExceptions = ValidationException.class)
    public void testInvalidProcessValidity() throws Exception {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getClusters().getClusters().get(0).getValidity().setStart(
                SchemaHelper.parseDateUTC("2011-12-31T00:00Z"));
        parser.validate(process);
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testInvalidDependentFeedsRetentionLimit() throws Exception {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getInputs().getInputs().get(0).setStart("today(-48,0)");
        parser.validate(process);
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testDuplicateInputOutputNames() throws FalconException {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getInputs().getInputs().get(0).setName("duplicateName");
        process.getOutputs().getOutputs().get(0).setName("duplicateName");
        parser.validate(process);
    }

    @Test(expectedExceptions = FalconException.class)
    public void testInvalidRetryAttempt() throws FalconException {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getRetry().setAttempts(-1);
        parser.parseAndValidate(process.toString());
    }

    @Test(expectedExceptions = FalconException.class)
    public void testInvalidRetryDelay() throws FalconException {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getRetry().setDelay(Frequency.fromString("hours(0)"));
        parser.parseAndValidate(process.toString());
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testInvalidLateInputs() throws Exception {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getLateProcess().getLateInputs().get(0).setInput("invalidInput");
        parser.parseAndValidate(process.toString());
    }

    @Test(expectedExceptions = FalconException.class)
    public void testInvalidProcessName() throws Exception {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.setName("name_with_underscore");
        parser.parseAndValidate(process.toString());
    }

    @Test
    public void testOozieFutureExpression() throws Exception {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getInputs().getInputs().get(0).setStart("future(1,2)");
        parser.parseAndValidate(process.toString());
    }

    @Test
    public void testOozieLatestExpression() throws Exception {
        Process process = parser
                .parseAndValidate((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getInputs().getInputs().get(0).setStart("latest(-1)");
        parser.parseAndValidate(process.toString());
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testDuplicateClusterName() throws Exception {
        Process process = parser
                .parse((ProcessEntityParserTest.class
                        .getResourceAsStream(PROCESS_XML)));
        process.getClusters().getClusters().add(1, process.getClusters().getClusters().get(0));
        parser.validate(process);
    }

    @Test
    public void testProcessForTableStorage() throws Exception {
        Feed inFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                this.getClass().getResource("/config/feed/hive-table-feed.xml"));
        getStore().publish(EntityType.FEED, inFeed);

        Feed outFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                this.getClass().getResource("/config/feed/hive-table-feed-out.xml"));
        getStore().publish(EntityType.FEED, outFeed);

        Process process = parser.parse(
                ProcessEntityParserTest.class.getResourceAsStream("/config/process/process-table.xml"));
        Input input = process.getInputs().getInputs().get(0);
        Assert.assertFalse(input.isOptional());
        parser.validate(process);

        // Test Optional Inputs For Table Storage
        try {
            input.setOptional(Boolean.TRUE);
            Assert.assertTrue(input.isOptional());
            parser.validate(process);
            Assert.fail("Validation exception must have been thrown.");
        } catch (FalconException e) {
            Assert.assertTrue(e instanceof ValidationException);
        }
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testValidateInputPartitionForTable() throws Exception {
        Process process = parser.parse(
                ProcessEntityParserTest.class.getResourceAsStream("/config/process/process-table.xml"));
        if (process.getInputs() != null) {
            for (Input input : process.getInputs().getInputs()) {
                input.setPartition("region=usa");
            }
        }

        parser.validate(process);
        Assert.fail("An exception should have been thrown since Input partitions are not supported for table storage");
    }
}
