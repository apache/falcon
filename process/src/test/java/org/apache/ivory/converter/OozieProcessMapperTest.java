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

package org.apache.ivory.converter;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interface;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.oozie.bundle.BUNDLEAPP;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.apache.ivory.oozie.workflow.ACTION;
import org.apache.ivory.oozie.workflow.FORK;
import org.apache.ivory.oozie.workflow.JOIN;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OozieProcessMapperTest extends AbstractTestBase{

    private String hdfsUrl;

    @BeforeClass
    public void setUpDFS() throws Exception {
        Configuration conf = new Configuration();
        new MiniDFSCluster(conf , 1, true, null);
        hdfsUrl = conf.get("fs.default.name");
    }
    
    @BeforeMethod
    public void setUp() throws Exception {
        super.setup();
        
        ConfigurationStore store = ConfigurationStore.get();
        Cluster cluster = store.get(EntityType.CLUSTER, "corp");
        Interface inter = new Interface();
        inter.setEndpoint(hdfsUrl);
        cluster.getInterfaces().put(Interfacetype.WRITE, inter);

        Process process = store.get(EntityType.PROCESS, "clicksummary");
        Path wfpath = new Path(process.getWorkflow().getPath());
        assert new Path(hdfsUrl).getFileSystem(new Configuration()).mkdirs(wfpath);
    }
    
    public void testDefCoordMap(Process process, COORDINATORAPP coord) throws Exception {
        assertEquals("IVORY_PROCESS_DEFAULT_" + process.getName(), coord.getName());
        assertEquals(process.getValidity().getStart(), coord.getStart());
        assertEquals(process.getValidity().getEnd(), coord.getEnd());
        assertEquals("${coord:"+process.getFrequency()+"("+process.getPeriodicity()+")}", coord.getFrequency());
        assertEquals(process.getValidity().getTimezone(), coord.getTimezone());
        
        assertEquals(process.getConcurrency()+"", coord.getControls().getConcurrency());
        assertEquals(process.getExecution(), coord.getControls().getExecution());
        
        assertEquals(process.getInputs().getInput().get(0).getName(), coord.getInputEvents().getDataIn().get(0).getName());
        assertEquals(process.getInputs().getInput().get(0).getName(), coord.getInputEvents().getDataIn().get(0).getDataset());
        assertEquals("${elext:"+process.getInputs().getInput().get(0).getStartInstance()+"}", coord.getInputEvents().getDataIn().get(0).getStartInstance());
        assertEquals("${elext:"+process.getInputs().getInput().get(0).getEndInstance()+"}", coord.getInputEvents().getDataIn().get(0).getEndInstance());
        
        assertEquals(process.getInputs().getInput().get(1).getName(), coord.getInputEvents().getDataIn().get(1).getName());
        assertEquals(process.getInputs().getInput().get(1).getName(), coord.getInputEvents().getDataIn().get(1).getDataset());
        assertEquals("${elext:"+process.getInputs().getInput().get(1).getStartInstance()+"}", coord.getInputEvents().getDataIn().get(1).getStartInstance());
        //TODO remove after EL fix for oozie el support
        assertEquals("${coord:"+process.getInputs().getInput().get(1).getEndInstance()+"}", coord.getInputEvents().getDataIn().get(1).getEndInstance());

        assertEquals(process.getOutputs().getOutput().get(0).getName(), coord.getOutputEvents().getDataOut().get(0).getName());
        assertEquals("${elext:"+process.getOutputs().getOutput().get(0).getInstance()+"}", coord.getOutputEvents().getDataOut().get(0).getInstance());
        assertEquals(process.getOutputs().getOutput().get(0).getName(), coord.getOutputEvents().getDataOut().get(0).getDataset());

        assertEquals(3, coord.getDatasets().getDatasetOrAsyncDataset().size());
        
        ConfigurationStore store = ConfigurationStore.get();
        Feed feed = store.get(EntityType.FEED, process.getInputs().getInput().get(0).getFeed());
        SYNCDATASET ds = (SYNCDATASET) coord.getDatasets().getDatasetOrAsyncDataset().get(0);
        assertEquals(feed.getClusters().getCluster().get(0).getValidity().getStart(), ds.getInitialInstance());
        assertEquals(feed.getClusters().getCluster().get(0).getValidity().getTimezone(), ds.getTimezone());
        assertEquals("${coord:"+feed.getFrequency()+"("+feed.getPeriodicity()+")}", ds.getFrequency());
        assertEquals("", ds.getDoneFlag());
        assertEquals("${nameNode}" +feed.getLocations().get(LocationType.DATA).getPath(), ds.getUriTemplate());        
    }
    
    @Test
    public void testBundle() throws Exception {
        Process process = ConfigurationStore.get().get(EntityType.PROCESS, "clicksummary");
        Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER, "corp");
        OozieProcessMapper mapper = new OozieProcessMapper(process);
        Path bundlePath = new Path("/", process.getStagingPath());
        mapper.map(cluster, bundlePath);
        
        FileSystem fs = new Path(hdfsUrl).getFileSystem(new Configuration());
        assertTrue(fs.exists(bundlePath));

        BUNDLEAPP bundle = getBundle(fs, bundlePath);
        assertEquals(process.getWorkflowName(), bundle.getName());
        assertEquals(1, bundle.getCoordinator().size());
        assertEquals(process.getWorkflowName("DEFAULT"), bundle.getCoordinator().get(0).getName());
        String coordPath = bundle.getCoordinator().get(0).getAppPath().replace("${nameNode}", "");
        
        COORDINATORAPP coord = getCoordinator(fs, new Path(coordPath));
        testDefCoordMap(process, coord);
        
        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        WORKFLOWAPP parentWorkflow = getParentWorkflow(fs, new Path(wfPath));
        testParentWorkflow(process,parentWorkflow);
    }
    
    public void testParentWorkflow(Process process, WORKFLOWAPP parentWorkflow){
    		Assert.assertEquals(process.getWorkflowName("DEFAULT"), parentWorkflow.getName());
    		//Assert.assertEquals("pre-processing", ((ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(0)).getName());
    		Assert.assertEquals("recordsize", ((ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(0)).getName());
    		Assert.assertEquals("user-workflow", ((ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(1)).getName());
    		Assert.assertEquals("fork-for-succeeded", ((FORK) parentWorkflow.getDecisionOrForkOrJoin().get(2)).getName());
    		Assert.assertEquals("join-for-succeeded", ((JOIN) parentWorkflow.getDecisionOrForkOrJoin().get(3)).getName());
    		Assert.assertEquals("fork-for-failed", ((FORK) parentWorkflow.getDecisionOrForkOrJoin().get(4)).getName());
    		Assert.assertEquals("join-for-failed", ((JOIN) parentWorkflow.getDecisionOrForkOrJoin().get(5)).getName());
    		Assert.assertEquals("ivory-succeeded-messaging", ((ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(6)).getName());
    		Assert.assertEquals("ivory-succeeded-log-mover", ((ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(7)).getName());
    		Assert.assertEquals("ivory-failed-log-mover", ((ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(8)).getName());
    		Assert.assertEquals("ivory-failed-messaging", ((ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(9)).getName());
    		Assert.assertEquals("user-jms-messaging", ((ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(10)).getName());
    }
    
    private COORDINATORAPP getCoordinator(FileSystem fs, Path path) throws Exception {
        String bundleStr = readFile(fs, new Path(path, "coordinator.xml"));
        
        Unmarshaller unmarshaller = JAXBContext.newInstance(COORDINATORAPP.class).createUnmarshaller();
        SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        Schema schema = schemaFactory.newSchema(this.getClass().getResource("/oozie-coordinator-0.3.xsd"));
        unmarshaller.setSchema(schema);
        JAXBElement<COORDINATORAPP> jaxbBundle = unmarshaller.unmarshal(new StreamSource(new ByteArrayInputStream(bundleStr.trim().getBytes())), COORDINATORAPP.class);
        return jaxbBundle.getValue();                
    }
    
    private WORKFLOWAPP getParentWorkflow(FileSystem fs, Path path) throws Exception {
        String workflow = readFile(fs, new Path(path, "workflow.xml"));
        
        Unmarshaller unmarshaller = JAXBContext.newInstance(WORKFLOWAPP.class).createUnmarshaller();
        SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        Schema schema = schemaFactory.newSchema(this.getClass().getResource("/oozie-workflow-0.3.xsd"));
        unmarshaller.setSchema(schema);
        JAXBElement<WORKFLOWAPP> jaxbWorkflow = unmarshaller.unmarshal(new StreamSource(new ByteArrayInputStream(workflow.trim().getBytes())), WORKFLOWAPP.class);
        return jaxbWorkflow.getValue();                
    }
    
    private BUNDLEAPP getBundle(FileSystem fs, Path path) throws Exception {
        String bundleStr = readFile(fs, new Path(path, "bundle.xml"));
        
        Unmarshaller unmarshaller = JAXBContext.newInstance(BUNDLEAPP.class).createUnmarshaller();
        SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        Schema schema = schemaFactory.newSchema(this.getClass().getResource("/oozie-bundle-0.1.xsd"));
        unmarshaller.setSchema(schema);
        JAXBElement<BUNDLEAPP> jaxbBundle = unmarshaller.unmarshal(new StreamSource(new ByteArrayInputStream(bundleStr.trim().getBytes())), BUNDLEAPP.class);
        return jaxbBundle.getValue();        
    }
    
    private String readFile(FileSystem fs, Path path) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        StringBuffer contents = new StringBuffer();
        while((line=reader.readLine()) != null) {
            contents.append(line);
        }
        return contents.toString();
    }
    
    @AfterClass
    public void cleanup() throws Exception{
    		super.cleanup();
    }
}