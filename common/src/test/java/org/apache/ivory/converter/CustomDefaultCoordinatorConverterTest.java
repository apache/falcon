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

import junit.framework.Assert;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.parser.ProcessEntityParser;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.ProcessType;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.dozer.DozerConverter;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CustomDefaultCoordinatorConverterTest {

	private ProcessType processType;
	private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

	@BeforeClass
	public void populateProcessType() throws IvoryException {
		ProcessEntityParser parser = (ProcessEntityParser) EntityParserFactory
				.getParser(EntityType.PROCESS);

		this.processType = (ProcessType) parser.parse(this.getClass()
				.getResourceAsStream(SAMPLE_PROCESS_XML));

	}

	@Test
	public void testConvert() {
		COORDINATORAPP coordinatorapp = new COORDINATORAPP();
		DozerConverter<ProcessType, COORDINATORAPP> converter = new CustomDefaultCoordinatorConverter();
		converter.convertTo(
				this.processType, coordinatorapp);
		Assert.assertEquals(coordinatorapp.getFrequency(), "${coord:" + processType.getFrequency()
				+ "(" + processType.getPeriodicity() + ")}");
	}
}
