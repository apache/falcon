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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;

import org.apache.ivory.Util;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class CoordinatorToProcessTest {

	private final Process process = new Process() ;
	private COORDINATORAPP coordinatorApp = new COORDINATORAPP();
	
	private static final String SAMPLE_COORDINATOR_XML = "/coordinator-agg.xml";
	
	@BeforeClass
	public void setup() throws JAXBException, SAXException{
		Unmarshaller unmarshaller = JAXBContext.newInstance(org.apache.ivory.oozie.coordinator.COORDINATORAPP.class).createUnmarshaller();
		Schema schema = Util.getSchema(CoordinatorToProcessTest.class
				.getResource("/coordinator.xsd"));
		unmarshaller.setSchema(schema);
		JAXBElement<COORDINATORAPP> coordinatorAppElement = (JAXBElement<COORDINATORAPP>) unmarshaller
				.unmarshal(CoordinatorToProcessTest.class
						.getResourceAsStream(SAMPLE_COORDINATOR_XML));
		coordinatorApp = coordinatorAppElement.getValue();	
	}
	
	@Test
	public void mapToProcess(){		
		DozerProvider.map(new String[] { "process-to-coordinator.xml"},
				this.coordinatorApp, this.process);

		DozerProvider.map(new String[] { "custom-process-to-coordinator.xml" },
				this.coordinatorApp, this.process);
		
		Assert.assertEquals(this.process.getName(), this.coordinatorApp.getName());
		
		
	}
	
	

}
