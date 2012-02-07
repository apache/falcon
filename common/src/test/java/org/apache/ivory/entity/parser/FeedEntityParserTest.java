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

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.ActionType;
import org.apache.ivory.entity.v0.feed.ClusterType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FeedEntityParserTest {

	private final FeedEntityParser parser = (FeedEntityParser) EntityParserFactory
			.getParser(EntityType.FEED);

	private static final String SAMPLE_DATASET_XML = "/config/feed/feed-0.1.xml";

	private static final String SAMPLE_INVALID_PROCESS_XML = "/process-invalid.xml";

	@Test
	public void testParse() throws IOException, IvoryException, JAXBException {
		
		Feed feed = (Feed) parser.parse(this.getClass().getResourceAsStream(
				SAMPLE_DATASET_XML));
		
		Assert.assertNotNull(feed);
		assertEquals(feed.getName(), "clicks");
		assertEquals(feed.getDescription(), "clicks log");
		assertEquals(feed.getFrequency(), "hourly");
		assertEquals(feed.getPeriodicity(), "1");
		assertEquals(feed.getGroups(),"online,bi");
		
		assertEquals(feed.getClusters().getCluster().get(0).getName(), "testCluster");
		assertEquals(feed.getClusters().getCluster().get(0).getType(), ClusterType.SOURCE);
		assertEquals(feed.getClusters().getCluster().get(0).getValidity().getStart(), "2011-11-01 00:00:00");
		assertEquals(feed.getClusters().getCluster().get(0).getValidity().getEnd() ,"9999-12-31 23:59:00");
		assertEquals(feed.getClusters().getCluster().get(0).getValidity().getTimezone() ,"UTC");
		assertEquals(feed.getClusters().getCluster().get(0).getRetention().getAction() ,ActionType.DELETE);
		assertEquals(feed.getClusters().getCluster().get(0).getRetention().getLimit() ,"hours(6)");

		assertEquals(feed.getClusters().getCluster().get(1).getName(), "backupCluster");
		assertEquals(feed.getClusters().getCluster().get(1).getType(), ClusterType.TARGET);
		assertEquals(feed.getClusters().getCluster().get(1).getValidity().getStart(), "2011-11-01 00:00:00");
		assertEquals(feed.getClusters().getCluster().get(1).getValidity().getEnd() ,"9999-12-31 23:59:00");
		assertEquals(feed.getClusters().getCluster().get(1).getValidity().getTimezone() ,"UTC");
		assertEquals(feed.getClusters().getCluster().get(1).getRetention().getAction() ,ActionType.ARCHIVE);
		assertEquals(feed.getClusters().getCluster().get(1).getRetention().getLimit() ,"hours(6)");
		
		assertEquals(feed.getLocations().get(LocationType.DATA).getType(),"data");
		assertEquals(feed.getLocations().get(LocationType.DATA).getPath(),"/projects/ivory/clicks");
		assertEquals(feed.getLocations().get(LocationType.META).getType(),"meta");
		assertEquals(feed.getLocations().get(LocationType.META).getPath(),"/projects/ivory/clicksMetaData");
		assertEquals(feed.getLocations().get(LocationType.STATS).getType(),"stats");
		assertEquals(feed.getLocations().get(LocationType.STATS).getPath(),"/projects/ivory/clicksStats");
		
		assertEquals(feed.getACL().getGroup(), "group");
		assertEquals(feed.getACL().getOwner(), "testuser");
		assertEquals(feed.getACL().getPermission(), "0x755");
		
		assertEquals(feed.getSchema().getLocation(), "/schema/clicks");
		assertEquals(feed.getSchema().getProvider(), "protobuf");
		
		assertEquals(feed.getProperties().get("field1")
				.getName(), "field1");
		assertEquals(feed.getProperties().get("field1")
				.getValue(), "value1");
		assertEquals(feed.getProperties().get("field2")
				.getName(), "field2");
		assertEquals(feed.getProperties().get("field2")
				.getValue(), "value2");
		
		StringWriter stringWriter = new StringWriter();
		Marshaller marshaller = Util.getMarshaller(Feed.class);
		marshaller.marshal(feed, stringWriter);
		System.out.println(stringWriter.toString());

	}

	//TODO
	@Test
	public void applyValidations() {
		// throw new RuntimeException("Test not implemented");
	}

}
