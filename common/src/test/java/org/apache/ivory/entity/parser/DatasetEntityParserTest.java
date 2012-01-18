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

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.dataset.Dataset;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatasetEntityParserTest {

	private final DatasetEntityParser parser = (DatasetEntityParser) EntityParserFactory
			.getParser(EntityType.DATASET);

	private static final String SAMPLE_DATASET_XML = "/resources/config/dataset/dataset.xml";

	private static final String SAMPLE_INVALID_PROCESS_XML = "/process-invalid.xml";

	@Test
	public void testParse() throws IOException, IvoryException {

		Dataset def = null;
		def = (Dataset) parser.parse(this.getClass().getResourceAsStream(
				SAMPLE_DATASET_XML));

		Assert.assertNotNull(def);

		Assert.assertEquals(def.getName(), "sample");

		//Assert.assertEquals(def.getValidity().getStart(), "2011-11-01 00:00:00");

	}

	

	
	//TODO
	@Test
	public void applyValidations() {
		// throw new RuntimeException("Test not implemented");
	}

}
