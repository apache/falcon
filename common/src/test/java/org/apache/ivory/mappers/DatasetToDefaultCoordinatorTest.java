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

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.parser.DatasetEntityParser;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.dataset.Dataset;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class DatasetToDefaultCoordinatorTest {
	private final COORDINATORAPP coordinatorapp = new COORDINATORAPP();
	private Dataset datasetA;
	private Dataset datasetB;
	private static final String SAMPLE_DATASET_A_XML = "/resources/config/dataset/dataset.xml";
	private static final String SAMPLE_DATASET_B_XML = "/resources/config/dataset/dataset2.xml";

	@BeforeClass
	public void populateDataset() throws IvoryException {
		DatasetEntityParser parser = (DatasetEntityParser) EntityParserFactory
				.getParser(EntityType.DATASET);

		this.datasetA = (Dataset) parser.parse(this.getClass()
				.getResourceAsStream(SAMPLE_DATASET_A_XML));
		
		this.datasetB = (Dataset) parser.parse(this.getClass()
				.getResourceAsStream(SAMPLE_DATASET_B_XML));

	}
	@Test
	public void testMap() throws JAXBException, SAXException {

		Map<Entity, EntityType> entityMap = new LinkedHashMap<Entity, EntityType>();
		entityMap.put(this.datasetA,EntityType.DATASET);
		entityMap.put(this.datasetB,EntityType.DATASET);
		// Map
		CoordinatorMapper coordinateMapper = new CoordinatorMapper(
				entityMap, this.coordinatorapp);
		coordinateMapper.mapToDefaultCoordinator();
		Assert.assertNotNull(coordinatorapp);
	}

}
