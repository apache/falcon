/**
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
package org.apache.falcon.cleanup;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.Path;

import java.util.Collection;

/**
 * Handler to cleanup files left behind by falcon relating to process.
 */
public class ProcessCleanupHandler extends AbstractCleanupHandler {

    @Override
    public void cleanup() throws FalconException {
        Collection<String> processes = STORE.getEntities(EntityType.PROCESS);
        for (String processName : processes) {
            Process process;
            process = STORE.get(EntityType.PROCESS, processName);
            long retention = getRetention(process, process.getFrequency()
                    .getTimeUnit());
            for (org.apache.falcon.entity.v0.process.Cluster cluster : process
                    .getClusters().getClusters()) {
                Cluster currentCluster = STORE.get(EntityType.CLUSTER,
                        cluster.getName());
                if (currentCluster.getColo().equals(getCurrentColo())) {
                    LOG.info("Cleaning up logs for process: {} in cluster: {} with retention: {}",
                            processName, cluster.getName(), retention);
                    delete(currentCluster, process, retention);
                } else {
                    LOG.info("Ignoring cleanup for process: {} in cluster: {} as this does not belong to current colo",
                            processName, cluster.getName());
                }
            }

        }
    }

    @Override
    protected Path getLogPath(Entity entity, String stagingPath) {
        Path logPath = new Path(stagingPath, "falcon/workflows/process/"
                + entity.getName() + "/logs/job-*/*");
        return logPath;
    }

}
