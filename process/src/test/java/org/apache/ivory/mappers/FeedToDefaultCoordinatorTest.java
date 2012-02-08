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
import org.apache.ivory.entity.parser.FeedEntityParser;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class FeedToDefaultCoordinatorTest {
	private final COORDINATORAPP coordinatorapp = new COORDINATORAPP();
	private Feed feedA;
	private Feed feedB;
	private static final String SAMPLE_DATASET_A_XML = "/config/feed/feed-A.xml";
	private static final String SAMPLE_DATASET_B_XML = "/config/feed/feed-B.xml";

	@BeforeClass
	public void populateFeed() throws IvoryException {
		FeedEntityParser parser = (FeedEntityParser) EntityParserFactory
				.getParser(EntityType.FEED);
		ConfigurationStore store = ConfigurationStore.get();
		
		Cluster prodCluster1 = new Cluster();
		prodCluster1.setName("testCluster");
		store.publish(EntityType.CLUSTER, prodCluster1);
		
		Cluster prodCluster2 = new Cluster();
		prodCluster2.setName("backupCluster");
		store.publish(EntityType.CLUSTER, prodCluster2);
		
		this.feedA = (Feed) parser.parse(this.getClass()
				.getResourceAsStream(SAMPLE_DATASET_A_XML));
		
		this.feedB = (Feed) parser.parse(this.getClass()
				.getResourceAsStream(SAMPLE_DATASET_B_XML));

	}
	@Test
	public void testMap() throws JAXBException, SAXException {

		Map<Entity, EntityType> entityMap = new LinkedHashMap<Entity, EntityType>();
		entityMap.put(this.feedA,EntityType.FEED);
		entityMap.put(this.feedB,EntityType.FEED);
		// Map
		CoordinatorMapper coordinateMapper = new CoordinatorMapper(
				entityMap, this.coordinatorapp);
		coordinateMapper.mapToDefaultCoordinator();
		Assert.assertNotNull(coordinatorapp);
		Assert.assertEquals(((SYNCDATASET)coordinatorapp.getDatasets().getDatasetOrAsyncDataset().get(0)).getName(), feedA.getName());
		Assert.assertEquals(((SYNCDATASET)coordinatorapp.getDatasets().getDatasetOrAsyncDataset().get(0)).getUriTemplate(), "${nameNode}"+feedA.getLocations().get(LocationType.DATA).getPath());
		Assert.assertEquals(((SYNCDATASET)coordinatorapp.getDatasets().getDatasetOrAsyncDataset().get(0)).getFrequency(), "${coord:"+feedA.getFrequency()+"("+feedA.getPeriodicity()+")}");
		Assert.assertEquals(((SYNCDATASET)coordinatorapp.getDatasets().getDatasetOrAsyncDataset().get(0)).getTimezone(), "UTC");
		Assert.assertEquals(((SYNCDATASET)coordinatorapp.getDatasets().getDatasetOrAsyncDataset().get(0)).getInitialInstance(), "2011-11-01 00:00:00");
	
		Assert.assertEquals(((SYNCDATASET)coordinatorapp.getDatasets().getDatasetOrAsyncDataset().get(1)).getName(), feedB.getName());
	}
	
	@AfterClass
	public void cleanup() throws StoreAccessException{
		ConfigurationStore store = ConfigurationStore.get();
		store.remove(EntityType.CLUSTER, "testCluster");
		store.remove(EntityType.CLUSTER, "backupCluster");
	}

}
