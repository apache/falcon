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
package org.apache.ivory.cleanup;

import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;

public class ProcessCleanupHandler extends AbstractCleanupHandler {

	@Override
	public void cleanup() throws IvoryException {
		Collection<String> processes = STORE.getEntities(EntityType.PROCESS);
		for (String processName : processes) {
			Process process;
			process = STORE.get(EntityType.PROCESS, processName);
			long retention = getRetention(process, process.getFrequency()
					.getTimeUnit());
			for (org.apache.ivory.entity.v0.process.Cluster cluster : process
					.getClusters().getClusters()) {
				LOG.info("Cleaning up logs for process:" + processName
						+ " in  cluster: " + cluster.getName() + " with retention: "+retention);
				Cluster currentCluster = STORE.get(EntityType.CLUSTER,
						cluster.getName());
				delete(currentCluster, process, retention);
			}

		}
	}

	@Override
	protected Path getLogPath(Entity entity, String stagingPath) {
		Path logPath = new Path(stagingPath, "ivory/workflows/process/"
				+ entity.getName() + "/logs/job-*/*");
		return logPath;
	}

}
