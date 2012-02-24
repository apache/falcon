/*
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

package org.apache.ivory.entity.parser;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.AbstractTestBase;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.Partition;
import org.apache.ivory.entity.v0.feed.Partitions;
import org.apache.ivory.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProcessEntityParserTest extends AbstractTestBase{

    private final ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory.getParser(EntityType.PROCESS);
    private String INVALID_PROCESS_XML = "/config/process/process-invalid.xml";

    @Test
    public void testNotNullgetUnmarshaller() throws Exception {
        final Unmarshaller unmarshaller = EntityType.PROCESS.getUnmarshaller();
        Assert.assertNotNull(unmarshaller);
    }

    @BeforeClass
    public void init() throws Exception {
        ProcessEntityParser.init();
    }
    
    @BeforeMethod
    public void setup() throws Exception {
        storeEntity(EntityType.PROCESS, "sample");
        storeEntity(EntityType.FEED, "impressionFeed");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "imp-click-join1");
        storeEntity(EntityType.FEED, "imp-click-join2");
        storeEntity(EntityType.CLUSTER, "testCluster");
    }

    @Test
    public void testParse() throws IOException, IvoryException, JAXBException {

        Process process = (Process) parser.parseAndValidate(ProcessEntityParserTest.class.getResourceAsStream(PROCESS_XML));

        Assert.assertNotNull(process);
        Assert.assertEquals(process.getName(), "sample");

        Assert.assertEquals(process.getConcurrency(), "1");
        Assert.assertEquals(process.getExecution(), "LIFO");
        Assert.assertEquals(process.getFrequency(), "hours");
        Assert.assertEquals(process.getPeriodicity(), "1");
        Assert.assertEquals(process.getEntityType(), EntityType.PROCESS);

        Assert.assertEquals(process.getInputs().getInput().get(0).getName(), "impression");
        Assert.assertEquals(process.getInputs().getInput().get(0).getFeed(), "impressionFeed");
        Assert.assertEquals(process.getInputs().getInput().get(0).getStartInstance(), "today(0,0)");
        Assert.assertEquals(process.getInputs().getInput().get(0).getEndInstance(), "today(2,0)");
        assertEquals(process.getInputs().getInput().get(0).getPartition(), "*/US");

        Assert.assertEquals(process.getOutputs().getOutput().get(0).getName(), "impOutput");
        Assert.assertEquals(process.getOutputs().getOutput().get(0).getFeed(), "imp-click-join1");
        Assert.assertEquals(process.getOutputs().getOutput().get(0).getInstance(), "today(0,0)");

        Assert.assertEquals(process.getProperties().getProperty().get(0).getName(), "name1");
        Assert.assertEquals(process.getProperties().getProperty().get(0).getValue(), "value1");

        Assert.assertEquals(process.getValidity().getStart(), "2011-11-02T00:00Z");
        Assert.assertEquals(process.getValidity().getEnd(), "2011-12-30T00:00Z");
        Assert.assertEquals(process.getValidity().getTimezone(), "UTC");

        Assert.assertEquals(process.getWorkflow().getEngine(), "oozie");
        Assert.assertEquals(process.getWorkflow().getPath(), "hdfs://path/to/workflow");
        Assert.assertEquals(process.getWorkflow().getLibpath(), "hdfs://path/to/workflow/lib");

        StringWriter stringWriter = new StringWriter();
        Marshaller marshaller = EntityType.PROCESS.getMarshaller();
        marshaller.marshal(process, stringWriter);
        System.out.println(stringWriter.toString());

        // TODO for retry and late policy
    }

    @Test
    public void testELExpressions() throws Exception {
        Process process = (Process) parser.parseAndValidate(ProcessEntityParserTest.class.getResourceAsStream(PROCESS_XML));
        process.getInputs().getInput().get(0).setStartInstance("lastMonth(0,0,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) { }

        process.getInputs().getInput().get(0).setStartInstance("today(0,0)");
        process.getInputs().getInput().get(0).setEndInstance("lastMonth(0,0,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) { }

        process.getInputs().getInput().get(0).setStartInstance("today(2,0)");
        process.getInputs().getInput().get(0).setEndInstance("today(0,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) { }

        process.getInputs().getInput().get(0).setStartInstance("today(0,0)");
        process.getInputs().getInput().get(0).setEndInstance("today(50,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) { }
        
        process.getInputs().getInput().get(0).setStartInstance("today(0,0)");
        process.getInputs().getInput().get(0).setEndInstance("today(2,0)");

        process.getOutputs().getOutput().get(0).setInstance("lastMonth(0,0,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) { }

        process.getOutputs().getOutput().get(0).setInstance("today(50,0)");
        try {
            parser.validate(process);
            throw new AssertionError("Expected ValidationException!");
        } catch (ValidationException e) { }
    }
    
    @Test(expectedExceptions = IvoryException.class)
    public void doParseInvalidXML() throws IOException, IvoryException {

        parser.parseAndValidate(this.getClass().getResourceAsStream(INVALID_PROCESS_XML ));
    }

    @Test(expectedExceptions = ValidationException.class)
    public void applyValidationInvalidProcess() throws Exception {
        Process process = (Process) parser.parseAndValidate(ProcessEntityParserTest.class.getResourceAsStream(PROCESS_XML));
        process.getClusters().getCluster().get(0).setName("invalid cluster");
        parser.validate(process);
    }

    @Test(expectedExceptions = IvoryException.class)
    public void testValidate() throws IvoryException {
        parser.parseAndValidate("<process></process>");
    }

    @Test
    public void testConcurrentParsing() throws Exception {
        List<Thread> threadList = new ArrayList<Thread>();
        
        for (int i = 0; i < 3; i++) {
            threadList.add(new Thread() {
                public void run() {
                    try {
                        ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory.getParser(EntityType.PROCESS);
                        parser.parseAndValidate(this.getClass().getResourceAsStream(PROCESS_XML));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        for(Thread thread:threadList) {
            thread.start();
        }
        for(Thread thread:threadList) {
            thread.join();
        }
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testInvalidPartition() throws Exception {
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(EntityType.FEED, "impressionFeed");

        Feed inputFeed1 = new Feed();
        Partitions parts = new Partitions();
        parts.getPartition().add(new Partition("carrier"));
        inputFeed1.setPartitions(parts);
        inputFeed1.setName("impressionFeed");
        store.publish(EntityType.FEED, inputFeed1);

        parser.parseAndValidate(ProcessEntityParserTest.class.getResourceAsStream(PROCESS_XML));
    }
}
