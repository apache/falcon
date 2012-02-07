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

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;

import java.util.Collection;

public class EntityIntegrityChecker {

    private static final ConfigurationStore configStore = ConfigurationStore.get();

    public static Entity referencedBy(EntityType type,
                                    Entity entity)
            throws IvoryException {

        switch (type) {
            default:
                return null;
            case FEED:
                return referencedBy((Feed) entity);
            case CLUSTER:
                return referencedBy((Cluster) entity);
        }
    }

    public static Entity referencedBy(Cluster cluster) throws IvoryException {

        Collection<String> entities = configStore.getEntities(EntityType.PROCESS);
        for (String entity : entities) {
            Process process = (Process) configStore.get(EntityType.PROCESS, entity);
            String clusterName = process.getClusters().getCluster().
                    get(0).getName();
            Cluster referredCluster = configStore.
                    get(EntityType.CLUSTER, clusterName);
            if (referredCluster != null && referredCluster.equals(cluster)) {
                return process;
            }
        }
        return null;
        //TODO check for dataset dependency
    }

    public static Entity referencedBy(Feed dataset) throws IvoryException {
        Collection<String> entities = configStore.getEntities(EntityType.PROCESS);
        for (String entity : entities) {
            Process process = configStore.get(EntityType.PROCESS, entity);
            for (Input input : process.getInputs().getInput()) {
                String datasetName = input.getFeed();
                Feed referredDataset = configStore.
                        get(EntityType.FEED, datasetName);
                if (referredDataset != null && referredDataset.equals(dataset)) {
                    return process;
                }
            }
            for (Output output : process.getOutputs().getOutput()) {
                String datasetName = output.getFeed();
                Feed referredDataset = configStore.
                        get(EntityType.FEED, datasetName);
                if (referredDataset != null && referredDataset.equals(dataset)) {
                    return process;
                }
            }
        }
        return null;
    }
}
