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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interface;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class OozieProcessMapperLateProcessTest {

	private static String hdfsUrl;
	private static final String CLUSTER_XML = "/config/late/late-cluster.xml";
	private static final String FEED1_XML = "/config/late/late-feed1.xml";
	private static final String FEED2_XML = "/config/late/late-feed2.xml";
	private static final String FEED3_XML = "/config/late/late-feed3.xml";
	private static final String PROCESS1_XML = "/config/late/late-process1.xml";
	private static final String PROCESS2_XML = "/config/late/late-process2.xml";
	private static final ConfigurationStore store = ConfigurationStore.get();
	private static MiniDFSCluster dfsCluster;
	private static 	Configuration conf = new Configuration();

	@BeforeClass
	public void setUpDFS() throws Exception {

		cleanupStore();

		conf = new Configuration();
		dfsCluster = new MiniDFSCluster(conf, 1, true, null);
		hdfsUrl = conf.get("fs.default.name");

		Cluster cluster = (Cluster) EntityType.CLUSTER.getUnmarshaller()
				.unmarshal(this.getClass().getResource(CLUSTER_XML));
		Interface inter = new Interface();
		inter.setEndpoint(hdfsUrl);
		inter.setType(Interfacetype.WRITE);
		inter.setVersion("1.2");
		cluster.getInterfaces().put(Interfacetype.WRITE, inter);

		store.publish(EntityType.CLUSTER, cluster);

		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				this.getClass().getResource(FEED1_XML));
		Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				this.getClass().getResource(FEED2_XML));
		Feed feed3 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				this.getClass().getResource(FEED3_XML));

		store.publish(EntityType.FEED, feed1);
		store.publish(EntityType.FEED, feed2);
		store.publish(EntityType.FEED, feed3);

		Process process1 = (Process) EntityType.PROCESS.getUnmarshaller()
				.unmarshal(this.getClass().getResource(PROCESS1_XML));
		store.publish(EntityType.PROCESS, process1);
		Process process2 = (Process) EntityType.PROCESS.getUnmarshaller()
				.unmarshal(this.getClass().getResource(PROCESS2_XML));
		store.publish(EntityType.PROCESS, process2);

	}

	private void cleanupStore() throws StoreAccessException {
		store.remove(EntityType.PROCESS, "late-process1");
		store.remove(EntityType.PROCESS, "late-process2");
		store.remove(EntityType.FEED, "late-feed1");
		store.remove(EntityType.FEED, "late-feed2");
		store.remove(EntityType.FEED, "late-feed3");
		store.remove(EntityType.CLUSTER, "late-cluster");

	}

	@Test
	public void testEmptyLateProcess() throws IvoryException {
		Process process = store.get(EntityType.PROCESS, "late-process1");
		Cluster cluster = store.get(EntityType.CLUSTER, "late-cluster");
		OozieProcessMapper mapper = new OozieProcessMapper(process);
		COORDINATORAPP coord = mapper.createLateCoordinator(cluster, new Path(
				hdfsUrl + "/late-coord1"));
		Assert.assertEquals(coord, null);
	}
	
	@Test
	public void testNonEmptyLateProcess() throws IvoryException, IOException {
		Process process = store.get(EntityType.PROCESS, "late-process2");
		Cluster cluster = store.get(EntityType.CLUSTER, "late-cluster");
		OozieProcessMapper mapper = new OozieProcessMapper(process);
		FileSystem.mkdirs(FileSystem.get(conf), new Path("/user/guest/workflow"), FsPermission.getDefault());
		COORDINATORAPP coord = mapper.createLateCoordinator(cluster, new Path(
				hdfsUrl + "/late-coord2"));
		Assert.assertEquals(coord.getName(), "IVORY_PROCESS_LATE1_late-process2");
	}

	@AfterClass
	public void tearDown() throws StoreAccessException {
		cleanupStore();
		dfsCluster.shutdown();
	}

}
