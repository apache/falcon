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

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.Partition;
import org.apache.ivory.entity.v0.feed.Partitions;
import org.apache.ivory.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class ProcessEntityParserTest {

    private final ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory.getParser(EntityType.PROCESS);
    private static final String SAMPLE_PROCESS_XML = "/config/process/process-version-0.xml";
    private static final String SAMPLE_INVALID_PROCESS_XML = "/config/process/process-invalid.xml";
    private static final String SAMPLE_FEED = "/config/feed/feed-0.1.xml";

    @Test
    public void testNotNullgetUnmarshaller() throws JAXBException {
        final Unmarshaller unmarshaller = Util.getUnmarshaller(parser.getEntityType().getEntityClass());

        Assert.assertNotNull(unmarshaller);
    }

    @BeforeMethod
    public void setup() throws Exception {
        cleanup();
        
        ConfigurationStore store = ConfigurationStore.get();
        Cluster prodCluster1 = new Cluster();
        prodCluster1.setName("testCluster");
        store.publish(EntityType.CLUSTER, prodCluster1);

        Unmarshaller unmarshaller = Util.jaxbContext.createUnmarshaller();
        Feed feed = (Feed) unmarshaller.unmarshal(this.getClass().getResourceAsStream(SAMPLE_FEED));
        feed.setName("impressionFeed");
        store.publish(EntityType.FEED, feed);

        feed = (Feed) unmarshaller.unmarshal(this.getClass().getResourceAsStream(SAMPLE_FEED));
        feed.setName("clicksFeed");
        store.publish(EntityType.FEED, feed);

        feed = (Feed) unmarshaller.unmarshal(this.getClass().getResourceAsStream(SAMPLE_FEED));
        feed.setName("imp-click-join1");
        store.publish(EntityType.FEED, feed);

        feed = (Feed) unmarshaller.unmarshal(this.getClass().getResourceAsStream(SAMPLE_FEED));
        feed.setName("imp-click-join2");
        store.publish(EntityType.FEED, feed);
    }

    @Test
    public void testParse() throws IOException, IvoryException, JAXBException {

        Process process = (Process) parser.parseAndValidate(ProcessEntityParserTest.class.getResourceAsStream(SAMPLE_PROCESS_XML));

        Assert.assertNotNull(process);
        Assert.assertEquals(process.getName(), "sample");

        Assert.assertEquals(process.getConcurrency(), "1");
        Assert.assertEquals(process.getExecution(), "LIFO");
        Assert.assertEquals(process.getFrequency(), "hourly");
        Assert.assertEquals(process.getPeriodicity(), "1");
        Assert.assertEquals(process.getEntityType(), EntityType.PROCESS);

        Assert.assertEquals(process.getInputs().getInput().get(0).getName(), "impression");
        Assert.assertEquals(process.getInputs().getInput().get(0).getFeed(), "impressionFeed");
        Assert.assertEquals(process.getInputs().getInput().get(0).getStartInstance(), "today(0,0)");
        Assert.assertEquals(process.getInputs().getInput().get(0).getEndInstance(), "today(0,2)");
        assertEquals(process.getInputs().getInput().get(0).getPartition(), "*/US");
        
        Assert.assertEquals(process.getOutputs().getOutput().get(0).getName(), "impOutput");
        Assert.assertEquals(process.getOutputs().getOutput().get(0).getFeed(), "imp-click-join1");
        Assert.assertEquals(process.getOutputs().getOutput().get(0).getInstance(), "today(0,0)");

        Assert.assertEquals(process.getProperties().getProperty().get(0).getName(), "name1");
        Assert.assertEquals(process.getProperties().getProperty().get(0).getValue(), "value1");

        Assert.assertEquals(process.getValidity().getStart(), "2011-11-01 00:00:00");
        Assert.assertEquals(process.getValidity().getEnd(), "9999-12-31 23:59:00");
        Assert.assertEquals(process.getValidity().getTimezone(), "UTC");

        Assert.assertEquals(process.getWorkflow().getEngine(), "oozie");
        Assert.assertEquals(process.getWorkflow().getPath(), "hdfs://path/to/workflow");
        Assert.assertEquals(process.getWorkflow().getLibpath(), "hdfs://path/to/workflow/lib");

        StringWriter stringWriter = new StringWriter();
        Marshaller marshaller = Util.getMarshaller(Process.class);
        marshaller.marshal(process, stringWriter);
        System.out.println(stringWriter.toString());

        // TODO for retry and late policy
    }

    @Test(expectedExceptions = IvoryException.class)
    public void doParseInvalidXML() throws IOException, IvoryException {

        parser.parseAndValidate(this.getClass().getResourceAsStream(SAMPLE_INVALID_PROCESS_XML));
    }

    @Test(expectedExceptions = ValidationException.class)
    public void applyValidationInvalidProcess() throws JAXBException, SAXException, StoreAccessException, ValidationException {
        Process process = (Process) parser.parseAndValidate(ProcessEntityParserTest.class.getResourceAsStream(SAMPLE_PROCESS_XML));
        process.getClusters().getCluster().get(0).setName("invalid cluster");
        parser.validate(process);
    }

    @Test(expectedExceptions = IvoryException.class)
    public void testValidate() throws IvoryException {
        parser.parse("<process></process>");
    }

    @Test
    public void testConcurrentParsing() throws IvoryException, InterruptedException {

        Thread thread1 = new Thread() {
            public void run() {
                try {
                    parser.parseAndValidate(this.getClass().getResourceAsStream(SAMPLE_PROCESS_XML));
                } catch (IvoryException e) {
                    e.printStackTrace();
                }
            }
        };

        Thread thread2 = new Thread() {
            public void run() {
                try {
                    parser.parseAndValidate(ProcessEntityParserTest.class
                            .getResourceAsStream(SAMPLE_PROCESS_XML));
                } catch (IvoryException e) {
                    e.printStackTrace();
                }
            }
        };

        Thread thread3 = new Thread() {
            public void run() {
                try {
                    parser.parseAndValidate(this.getClass().getResourceAsStream(SAMPLE_PROCESS_XML));
                } catch (IvoryException e) {
                    e.printStackTrace();
                }
            }
        };
        thread1.start();
        thread2.start();
        thread3.start();
        Thread.sleep(5000);
    }
    
    @Test(expectedExceptions=ValidationException.class)
    public void testInvalidPartition() throws Exception {
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(EntityType.FEED, "impressionFeed");
        
        Feed inputFeed1 = new Feed();
        Partitions parts = new Partitions();
        parts.getPartition().add(new Partition("carrier"));
        inputFeed1.setPartitions(parts);
        inputFeed1.setName("impressionFeed");
        store.publish(EntityType.FEED, inputFeed1);

        parser.parseAndValidate(ProcessEntityParserTest.class.getResourceAsStream(SAMPLE_PROCESS_XML));
    }

    public void cleanup() throws StoreAccessException {
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(EntityType.PROCESS, "sample");
        store.remove(EntityType.FEED, "impressionFeed");
        store.remove(EntityType.FEED, "clicksFeed");
        store.remove(EntityType.FEED, "imp-click-join1");
        store.remove(EntityType.FEED, "imp-click-join2");
        store.remove(EntityType.CLUSTER, "testCluster");
    }
}
