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

/**
 * Test Cases for ProcessEntityParser
 */
import java.io.IOException;
import java.io.StringWriter;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProcessEntityParserTest {

	private final ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory
			.getParser(EntityType.PROCESS);

	private static final String SAMPLE_PROCESS_XML = "/config/process/process-version-0.xml";

	private static final String SAMPLE_INVALID_PROCESS_XML = "/config/process/process-invalid.xml";

	@Test
	public void testNotNullgetUnmarshaller() throws JAXBException {
		final Unmarshaller unmarshaller = EntityParser.EntityUnmarshaller
				.getInstance(parser.getEntityType(), parser.getClazz());

		Assert.assertNotNull(unmarshaller);
	}

	@Test
	public void testIsSingletonUnmarshaller() throws JAXBException {
		final Unmarshaller unmarshaller1 = EntityParser.EntityUnmarshaller
				.getInstance(parser.getEntityType(), parser.getClazz());

		final Unmarshaller unmarshaller2 = EntityParser.EntityUnmarshaller
				.getInstance(parser.getEntityType(), parser.getClazz());

		Assert.assertEquals(unmarshaller1, unmarshaller2);

	}

	@Test
	public void testParse() throws IOException, IvoryException, JAXBException {
		
		 Process process = (Process) parser.parse(ProcessEntityParserTest.class.getResourceAsStream(
				SAMPLE_PROCESS_XML));

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
		
		Assert.assertEquals(process.getOutputs().getOutput().get(0).getName(),"impOutput");
		Assert.assertEquals(process.getOutputs().getOutput().get(0).getFeed(),"imp-click-join");
		Assert.assertEquals(process.getOutputs().getOutput().get(0).getInstance(),"today(0,0)");
		
//		Assert.assertEquals(process.getProperties().get("name1").getName(),"name1");
//		Assert.assertEquals(process.getProperties().get("name1").getValue(),"value1");		
		Assert.assertEquals(process.getProperties().getProperty().get(0).getName(), "name1");
		Assert.assertEquals(process.getProperties().getProperty().get(0).getValue(), "value1");
		
		Assert.assertEquals(process.getValidity().getStart(), "2011-11-01 00:00:00");
		Assert.assertEquals(process.getValidity().getEnd(), "9999-12-31 23:59:00");	
		Assert.assertEquals(process.getValidity().getTimezone(), "UTC");	
		
		Assert.assertEquals(process.getWorkflow().getEngine(),"oozie");
		Assert.assertEquals(process.getWorkflow().getPath(),"hdfs://path/to/workflow");
		Assert.assertEquals(process.getWorkflow().getLibpath(),"hdfs://path/to/workflow/lib");
		
		StringWriter stringWriter = new StringWriter();
		Marshaller marshaller = Util.getMarshaller(Process.class);
		marshaller.marshal(process, stringWriter);
		System.out.println(stringWriter.toString());
		
		//TODO for retry and late policy
	}

	@Test(expectedExceptions = IvoryException.class)
	public void doParseInvalidXML() throws IOException, IvoryException {

		parser.parse(this.getClass().getResourceAsStream(
				SAMPLE_INVALID_PROCESS_XML));
	}
	
	@Test(expectedExceptions=IvoryException.class)
	public void testValidate() throws IvoryException{
		parser.validateSchema("<process></process>");
	}

	@Test(expectedExceptions=IvoryException.class)
	public void testParseString() throws IvoryException{
		parser.parse("<process></process>");
	}
	
	@Test
	public void testConcurrentParsing() throws IvoryException, InterruptedException{
		
		Thread thread = new Thread(){
			public void run() {
				Process def = null;
				try {
					def = (Process) parser.parse(this.getClass().getResourceAsStream(
							SAMPLE_PROCESS_XML));
				} catch (IvoryException e) {
					e.printStackTrace();
				}
			}
		};
		
		Thread thread1 = new Thread(){
			public void run() {
				Process def = null;
				try {
					def = (Process) parser.parse(ProcessEntityParserTest.class.getResourceAsStream(
							SAMPLE_PROCESS_XML));
				} catch (IvoryException e) {
					e.printStackTrace();
				}
			}
		};
		
		Thread thread2 = new Thread(){
			public void run() {
				Process def = null;
				try {
					def = (Process) parser.parse(this.getClass().getResourceAsStream(
							SAMPLE_PROCESS_XML));
				} catch (IvoryException e) {
					e.printStackTrace();
				}
			}
		};
		
		thread1.start();
		thread2.start();
		thread.start();
		
		Thread.sleep(5000);
		
	}
	
	//TODO
	@Test
	public void applyValidations() {
		// throw new RuntimeException("Test not implemented");
	}

}
