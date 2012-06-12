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

package org.apache.ivory.entity.v0;

import org.apache.ivory.entity.v0.process.Cluster;
import org.apache.ivory.entity.v0.process.Clusters;
import org.apache.ivory.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestEntity {
	@Test
	public void testDeepEquals() {
		Process process1 = createProcess("process", "cluster");
		Process process2 = createProcess("process", "cluster");

		Assert.assertTrue(process1.deepEquals(process2));

		process2.getClusters().getClusters().get(0).setName("cluster2");
		Assert.assertFalse(process1.deepEquals(process2));
	}

	private Process createProcess(String name, String clusterName) {
		Process process = new Process();
		process.setName(name);
		Cluster cluster = new Cluster();
		cluster.setName(clusterName);
		process.setClusters(new Clusters());
		process.getClusters().getClusters().add(cluster);
		return process;
	}

	@Test
	public void testEntityNullEquals() {
		Entity entity = new Entity() {

			@Override
			public String getName() {
				return "Entity";
			}
		};
		Entity nullEntity = null;
		try {
			if (entity.equals(nullEntity)) {
			}
		} catch (Exception e) {
			Assert.fail("Not expecting exception:"+e);
		}
	}
}
