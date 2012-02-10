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

import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.mappers.CoordinatorMapper;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class ProcessConverterTest {

    private Process process;
    private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

    public void cleanup() throws StoreAccessException {
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(EntityType.PROCESS, "sample");
        store.remove(EntityType.FEED, "impressions");
        store.remove(EntityType.FEED, "clicks");
        store.remove(EntityType.FEED, "imp-click-join1");
        store.remove(EntityType.FEED, "imp-click-join2");
        store.remove(EntityType.CLUSTER, "testCluster");
    }

    @BeforeMethod
    public void setup() throws Exception {
        cleanup();
        
        ConfigurationStore store = ConfigurationStore.get();
        Cluster prodCluster1 = new Cluster();
        prodCluster1.setName("testCluster");
        store.publish(EntityType.CLUSTER, prodCluster1);

        Unmarshaller unmarshaller = Util.getUnmarshaller(Feed.class);
        Feed inputFeed1 = (Feed) unmarshaller.unmarshal(this.getClass().getResourceAsStream("/config/feed/feed-A.xml"));
        store.publish(EntityType.FEED, inputFeed1);

        Feed inputFeed2 = (Feed) unmarshaller.unmarshal(this.getClass().getResourceAsStream("/config/feed/feed-B.xml"));
        store.publish(EntityType.FEED, inputFeed2);

        Feed outputFeed1 = (Feed) unmarshaller.unmarshal(this.getClass().getResourceAsStream("/config/feed/feed-A.xml"));
        outputFeed1.setName("imp-click-join1");
        store.publish(EntityType.FEED, outputFeed1);

        Feed outputFeed2 = (Feed) unmarshaller.unmarshal(this.getClass().getResourceAsStream("/config/feed/feed-A.xml"));
        outputFeed2.setName("imp-click-join2");
        store.publish(EntityType.FEED, outputFeed2);
    }

    @BeforeClass
    public void populateProcess() throws IvoryException, JAXBException {
        Unmarshaller unmarshaller = Util.getUnmarshaller(Process.class);
        process = (org.apache.ivory.entity.v0.process.Process) unmarshaller
                .unmarshal(this.getClass().getResourceAsStream(
                        SAMPLE_PROCESS_XML));
    }

    @Test
    public void testMap() throws JAXBException, SAXException {

        Map<Entity, EntityType> entityMap = new LinkedHashMap<Entity, EntityType>();
        entityMap.put(this.process, EntityType.PROCESS);

        // Map
        COORDINATORAPP coordinatorapp = CoordinatorMapper.mapToCoordinator(process);

        assertEquals(coordinatorapp.getName(), this.process.getName());
        assertEquals(coordinatorapp.getStart(), this.process.getValidity().getStart());
        assertEquals(coordinatorapp.getEnd(), this.process.getValidity().getEnd());
        //custom mapping
        assertEquals(coordinatorapp.getFrequency(), "${coord:"+this.process.getFrequency()+"("+this.process.getPeriodicity()+")}");
        //assertEquals(this.coordinatorapp.getTimezone(), this.process.);
        
        assertEquals(coordinatorapp.getControls().getConcurrency(), this.process.getConcurrency());
        assertEquals(coordinatorapp.getControls().getExecution(), this.process.getExecution());
        //assertEquals(this.coordinatorapp.getControls().getThrottle(), this.process.);
        //assertEquals(this.coordinatorapp.getControls().getTimeout(), "");
        
        assertEquals(coordinatorapp.getInputEvents().getDataIn().get(0).getName(), this.process.getInputs().getInput().get(0).getName());
        assertEquals(coordinatorapp.getInputEvents().getDataIn().get(0).getDataset(), this.process.getInputs().getInput().get(0).getFeed());
        assertEquals(coordinatorapp.getInputEvents().getDataIn().get(0).getStartInstance(), "${ivory:"+this.process.getInputs().getInput().get(0).getStartInstance()+"}");
        assertEquals(coordinatorapp.getInputEvents().getDataIn().get(0).getEndInstance(),  "${ivory:"+this.process.getInputs().getInput().get(0).getEndInstance()+"}");

        assertEquals(coordinatorapp.getOutputEvents().getDataOut().get(0).getName(),this.process.getOutputs().getOutput().get(0).getName());
        assertEquals(coordinatorapp.getOutputEvents().getDataOut().get(0).getInstance(), "${ivory:"+this.process.getOutputs().getOutput().get(0).getInstance()+"}");
        assertEquals(coordinatorapp.getOutputEvents().getDataOut().get(0).getDataset(),this.process.getOutputs().getOutput().get(0).getFeed());

        assertEquals(4, coordinatorapp.getDatasets().getDatasetOrAsyncDataset().size());
        for(Object dsObj:coordinatorapp.getDatasets().getDatasetOrAsyncDataset()) {
            SYNCDATASET ds = (SYNCDATASET) dsObj;
            if(ds.getName().equals("clicks")) {
                assertTrue(ds.getUriTemplate().endsWith("*/US"));
            }
        }

        coordinatorapp.setFrequency("5");

        Marshaller marshaller = JAXBContext.newInstance(COORDINATORAPP.class).createMarshaller();
        marshaller.setSchema(Util.getSchema(this.getClass().getResource("/coordinator.xsd")));
        marshaller.marshal(new ObjectFactory().createCoordinatorApp(coordinatorapp), System.out);
    }
}
