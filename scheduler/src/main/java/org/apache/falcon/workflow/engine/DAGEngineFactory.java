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
package org.apache.falcon.workflow.engine;

import org.apache.falcon.FalconException;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.entity.v0.cluster.Cluster;

import java.util.HashMap;
import java.util.Map;

/**
 *  Factory for providing appropriate dag engine to the Falcon services.
 */
public final class DAGEngineFactory {
    private static final String DAG_ENGINE = "dag.engine.impl";

    // Cache the DAGEngines, to avoid overhead of creation.
    private static final Map<String, DAGEngine> DAG_ENGINES = new HashMap<>();

    private DAGEngineFactory() {
    }

    public static DAGEngine getDAGEngine(Cluster cluster) throws FalconException {
        return getDAGEngine(cluster.getName());
    }

    public static DAGEngine getDAGEngine(String clusterName) throws FalconException {
        // Cache the DAGEngines for every cluster.
        if (!DAG_ENGINES.containsKey(clusterName)) {
            DAG_ENGINES.put(clusterName,
                    (DAGEngine) ReflectionUtils.getInstance(DAG_ENGINE, String.class, clusterName));
        }

        return DAG_ENGINES.get(clusterName);
    }

}
