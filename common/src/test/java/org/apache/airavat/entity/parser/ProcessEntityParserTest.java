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

package org.apache.airavat.entity.parser;

/**
 * Test Cases for ProcessEntityParser
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.bind.UnmarshalException;
import javax.xml.bind.Unmarshaller;

import org.apache.airavat.entity.v0.EntityType;
import org.apache.airavat.entity.v0.ProcessType;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class ProcessEntityParserTest {

	private final ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory
			.getParser(EntityType.PROCESS);

	private static final String SAMPLE_PROCESS_XML = "src/test/resources/process-version-0.xml";

	private static final String SAMPLE_INVALID_PROCESS_XML = "src/test/resources/process-invalid.xml";

	@Test
	public void testNotNullgetUnmarshaller() throws JAXBException {
		Unmarshaller unmarshaller = EntityParser.EntityUnmarshaller
				.getInstance(parser.getEntityType(), parser.getClazz());

		Assert.assertNotNull(unmarshaller);
	}
	
	@Test
	public void testIsSingletonUnmarshaller() throws JAXBException{
		Unmarshaller unmarshaller1 = EntityParser.EntityUnmarshaller
				.getInstance(parser.getEntityType(), parser.getClazz());
		
		Unmarshaller unmarshaller2 = EntityParser.EntityUnmarshaller
				.getInstance(parser.getEntityType(), parser.getClazz());

		Assert.assertEquals(unmarshaller1,unmarshaller2);

	}

	@Test
	public void doParse() throws IOException {
		String processXML = readFileAsString(SAMPLE_PROCESS_XML);
		// System.out.println(processXML);
		ProcessType def = null;
		try {
			def = parser.doParse(processXML);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Assert.assertNotNull(def);

		Assert.assertEquals(def.getName(), "sample");

		Assert.assertEquals(def.getValidity().getStart(), "2011-11-01 00:00:00");

	}

	@Test(expectedExceptions = UnmarshalException.class)
	public void doParseInvalidXML() throws IOException, SAXException,
			JAXBException {
		String processXML = readFileAsString(SAMPLE_INVALID_PROCESS_XML);
		// System.out.println(processXML);
		ProcessType def = parser.doParse(processXML);
	}

	@Test
	public void applyValidations() {
		// throw new RuntimeException("Test not implemented");
	}

	private static String readFileAsString(String filePath)
			throws java.io.IOException {
		StringBuffer fileData = new StringBuffer(1000);
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		char[] buf = new char[1024];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			String readData = String.valueOf(buf, 0, numRead);
			fileData.append(readData);
			buf = new char[1024];
		}
		reader.close();
		return fileData.toString();
	}
}
