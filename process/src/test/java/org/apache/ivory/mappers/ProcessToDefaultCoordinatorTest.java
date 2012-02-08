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

package org.apache.ivory.mappers;

import static org.testng.AssertJUnit.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class ProcessToDefaultCoordinatorTest {

    private final COORDINATORAPP coordinatorapp = new COORDINATORAPP();
    private Process process;
    private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

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
        CoordinatorMapper coordinateMapper = new CoordinatorMapper(entityMap, this.coordinatorapp);
        coordinateMapper.mapToDefaultCoordinator();

        Assert.assertNotNull(coordinateMapper.getEntityMap());
        Assert.assertNotNull(coordinateMapper.getCoordinatorapp());
        assertEquals(this.coordinatorapp.getName(), this.process.getName());
        assertEquals(this.coordinatorapp.getStart(), this.process.getValidity().getStart());
        assertEquals(this.coordinatorapp.getEnd(), this.process.getValidity().getEnd());
        //custom mapping
        assertEquals(this.coordinatorapp.getFrequency(), "${coord:"+this.process.getFrequency()+"("+this.process.getPeriodicity()+")}");
        //assertEquals(this.coordinatorapp.getTimezone(), this.process.);
        
        assertEquals(this.coordinatorapp.getControls().getConcurrency(), this.process.getConcurrency());
        assertEquals(this.coordinatorapp.getControls().getExecution(), this.process.getExecution());
        //assertEquals(this.coordinatorapp.getControls().getThrottle(), this.process.);
        //assertEquals(this.coordinatorapp.getControls().getTimeout(), "");
        
        assertEquals(this.coordinatorapp.getInputEvents().getDataIn().get(0).getName(), this.process.getInputs().getInput().get(0).getName());
        assertEquals(this.coordinatorapp.getInputEvents().getDataIn().get(0).getDataset(), this.process.getInputs().getInput().get(0).getFeed());
        assertEquals(this.coordinatorapp.getInputEvents().getDataIn().get(0).getStartInstance(), "${ivory:"+this.process.getInputs().getInput().get(0).getStartInstance()+"}");
        assertEquals(this.coordinatorapp.getInputEvents().getDataIn().get(0).getEndInstance(),  "${ivory:"+this.process.getInputs().getInput().get(0).getEndInstance()+"}");

        assertEquals(coordinatorapp.getOutputEvents().getDataOut().get(0).getName(),this.process.getOutputs().getOutput().get(0).getName());
        assertEquals(coordinatorapp.getOutputEvents().getDataOut().get(0).getInstance(), "${ivory:"+this.process.getOutputs().getOutput().get(0).getInstance()+"}");
        assertEquals(coordinatorapp.getOutputEvents().getDataOut().get(0).getDataset(),this.process.getOutputs().getOutput().get(0).getFeed());

        Marshaller marshaller = Util.getMarshaller(COORDINATORAPP.class);

        this.coordinatorapp.setFrequency("5");

        marshaller.setSchema(Util.getSchema(ProcessToDefaultCoordinatorTest.class.getResource("/coordinator.xsd")));
        marshaller.marshal(new ObjectFactory().createCoordinatorApp(this.coordinatorapp), System.out);
    }
}
