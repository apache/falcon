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

package org.apache.falcon.entity.v0;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory graph of entities and relationship among themselves.
 */
public final class EntityGraph implements ConfigurationChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(EntityGraph.class);

    private static EntityGraph instance = new EntityGraph();

    private Map<Node, Set<Node>> graph = new ConcurrentHashMap<Node, Set<Node>>();

    private EntityGraph() {
    }

    public static EntityGraph get() {
        return instance;
    }

    public Set<Entity> getDependents(Entity entity) throws FalconException {
        Node entityNode = new Node(entity.getEntityType(), entity.getName());
        if (graph.containsKey(entityNode)) {
            ConfigurationStore store = ConfigurationStore.get();
            Set<Entity> dependents = new HashSet<Entity>();
            for (Node node : graph.get(entityNode)) {
                Entity dependentEntity = store.get(node.type, node.name);
                if (dependentEntity != null) {
                    dependents.add(dependentEntity);
                } else {
                    LOG.error("Dependent entity {} was not found in configuration store.", node);
                }
            }
            return dependents;
        } else {
            return null;
        }
    }

    @Override
    public void onAdd(Entity entity) throws FalconException {
        Map<Node, Set<Node>> nodeEdges = null;
        switch (entity.getEntityType()) {
        case PROCESS:
            nodeEdges = getEdgesFor((Process) entity);
            break;
        case FEED:
            nodeEdges = getEdgesFor((Feed) entity);
            break;
        default:
        }
        if (nodeEdges == null) {
            return;
        }
        LOG.debug("Adding edges for {}: {}", entity.getName(), nodeEdges);

        for (Map.Entry<Node, Set<Node>> entry : nodeEdges.entrySet()) {
            LOG.debug("Adding edges : {} for {}", entry.getValue(), entry.getKey());
            if (graph.containsKey(entry.getKey())) {
                graph.get(entry.getKey()).addAll(entry.getValue());
            } else {
                graph.put(entry.getKey(), entry.getValue());
            }
        }
        LOG.debug("Merged edges to graph {}", entity.getName());
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        Map<Node, Set<Node>> nodeEdges = null;
        switch (entity.getEntityType()) {
        case PROCESS:
            nodeEdges = getEdgesFor((Process) entity);
            break;
        case FEED:
            nodeEdges = getEdgesFor((Feed) entity);
            break;
        default:
        }
        if (nodeEdges == null) {
            return;
        }

        for (Map.Entry<Node, Set<Node>> entry : nodeEdges.entrySet()) {
            if (graph.containsKey(entry.getKey())) {
                graph.get(entry.getKey()).removeAll(entry.getValue());
                if (graph.get(entry.getKey()).isEmpty()) {
                    graph.remove(entry.getKey());
                }
            }
        }
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        onRemove(oldEntity);
        onAdd(newEntity);
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        onAdd(entity);
    }

    private Map<Node, Set<Node>> getEdgesFor(Process process) {
        Map<Node, Set<Node>> nodeEdges = new HashMap<Node, Set<Node>>();
        Node processNode = new Node(EntityType.PROCESS, process.getName());
        nodeEdges.put(processNode, new HashSet<Node>());
        Set<Node> processEdges = nodeEdges.get(processNode);
        if (process.getInputs() != null) {
            for (Input input : process.getInputs().getInputs()) {
                Node feedNode = new Node(EntityType.FEED, input.getFeed());
                if (!nodeEdges.containsKey(feedNode)) {
                    nodeEdges.put(feedNode, new HashSet<Node>());
                }
                Set<Node> feedEdges = nodeEdges.get(feedNode);
                processEdges.add(feedNode);
                feedEdges.add(processNode);
            }
        }
        if (process.getOutputs() != null) {
            for (Output output : process.getOutputs().getOutputs()) {
                Node feedNode = new Node(EntityType.FEED, output.getFeed());
                if (!nodeEdges.containsKey(feedNode)) {
                    nodeEdges.put(feedNode, new HashSet<Node>());
                }
                Set<Node> feedEdges = nodeEdges.get(feedNode);
                processEdges.add(feedNode);
                feedEdges.add(processNode);
            }
        }

        for (Cluster cluster : process.getClusters().getClusters()) {
            Node clusterNode = new Node(EntityType.CLUSTER, cluster.getName());
            processEdges.add(clusterNode);
            nodeEdges.put(clusterNode, new HashSet<Node>());
            nodeEdges.get(clusterNode).add(processNode);
        }

        return nodeEdges;
    }

    private Map<Node, Set<Node>> getEdgesFor(Feed feed) {
        Map<Node, Set<Node>> nodeEdges = new HashMap<Node, Set<Node>>();
        Node feedNode = new Node(EntityType.FEED, feed.getName());
        Set<Node> feedEdges = new HashSet<Node>();
        nodeEdges.put(feedNode, feedEdges);

        for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed.getClusters().getClusters()) {
            Node clusterNode = new Node(EntityType.CLUSTER, cluster.getName());
            if (!nodeEdges.containsKey(clusterNode)) {
                nodeEdges.put(clusterNode, new HashSet<Node>());
            }
            Set<Node> clusterEdges = nodeEdges.get(clusterNode);
            feedEdges.add(clusterNode);
            clusterEdges.add(feedNode);

            if (FeedHelper.isImportEnabled(cluster)) {
                Node dbNode = new Node(EntityType.DATASOURCE, FeedHelper.getImportDatasourceName(cluster));
                if (!nodeEdges.containsKey(dbNode)) {
                    nodeEdges.put(dbNode, new HashSet<Node>());
                }
                Set<Node> dbEdges = nodeEdges.get(dbNode);
                feedEdges.add(dbNode);
                dbEdges.add(feedNode);
            }
        }
        return nodeEdges;
    }

    /**
     * Node element in the graph.
     */
    private static final class Node {

        private final EntityType type;
        private final String name;

        private Node(EntityType type, String name) {
            this.type = type;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Node node = (Node) o;

            boolean nameEqual = name != null ? !name.equals(node.name) : node.name != null;

            if (nameEqual) {
                return false;
            }
            if (type != node.type) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "(" + type + ") " + name;
        }
    }
}
