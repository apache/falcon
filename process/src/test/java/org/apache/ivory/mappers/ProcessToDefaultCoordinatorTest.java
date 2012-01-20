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

import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.parser.ProcessEntityParser;
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
	private Process Process;
	private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

	@BeforeClass
	public void populateProcess() throws IvoryException {
		ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory
				.getParser(EntityType.PROCESS);

		this.Process = (Process) parser.parse(this.getClass()
				.getResourceAsStream(SAMPLE_PROCESS_XML));

	}

	@Test
	public void testMap() throws JAXBException, SAXException {

		Map<Entity,EntityType> entityMap = new LinkedHashMap<Entity,EntityType>();
		entityMap.put(this.Process,EntityType.PROCESS);
		// Map
		CoordinatorMapper coordinateMapper = new CoordinatorMapper(
				entityMap, this.coordinatorapp);
		coordinateMapper.mapToDefaultCoordinator();

		Assert.assertNotNull(coordinateMapper.getEntityMap());

		Assert.assertNotNull(coordinateMapper.getCoordinatorapp());

		Assert.assertEquals(this.coordinatorapp.getName(),
				this.Process.getName());

		Assert.assertEquals(this.coordinatorapp.getControls().getConcurrency(),
				this.Process.getConcurrency());

		Assert.assertEquals(this.coordinatorapp.getControls().getExecution(),
				this.Process.getExecution());

		Assert.assertEquals(this.coordinatorapp.getStart(), this.Process
				.getValidity().getStart());

		Assert.assertEquals(this.coordinatorapp.getEnd(), this.Process
				.getValidity().getEnd());
		
		//custom mapping
		Assert.assertEquals(this.coordinatorapp.getFrequency(), "${coord:" + Process.getFrequency()
				+ "(" + this.Process.getPeriodicity() + ")}");

		Marshaller marshaller = Util.getMarshaller(COORDINATORAPP.class);

		this.coordinatorapp.setFrequency("5");

		marshaller.setSchema(Util.getSchema(ProcessToDefaultCoordinatorTest.class
				.getResource("/coordinator.xsd")));
		marshaller.marshal(
				new ObjectFactory().createCoordinatorApp(this.coordinatorapp),
				System.out);

	}

}
