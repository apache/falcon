package org.apache.ivory.entity.v0;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.service.ConfigurationChangeListener;
import org.apache.ivory.service.IvoryService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: sriksun
 * Date: 18/03/12
 */
public class EntityGraph implements
        ConfigurationChangeListener, IvoryService {


    private static EntityGraph instance = new EntityGraph();

    private Map<Node, Set<Node>> graph =
            new ConcurrentHashMap<Node, Set<Node>>();

    public static EntityGraph get() {
        return instance;
    }

    public Set<Entity> getDependents(Entity entity) throws IvoryException {
        Node entityNode = new Node(entity.getEntityType(), entity.getName());
        if (graph.containsKey(entityNode)) {
            ConfigurationStore store = ConfigurationStore.get();
            Set<Entity> dependents = new HashSet<Entity>();
            for (Node node: graph.get(entityNode)) {
                Entity dependentEntity = store.get(node.type, node.name);
                assert dependentEntity != null : "Unable to find " + node;
                dependents.add(dependentEntity);
            }
            return dependents;
        } else {
            return null;
        }
    }

    @Override
    public void onAdd(Entity entity) throws IvoryException {
        Map<Node, Set<Node>> nodeEdges = null;
        switch (entity.getEntityType()) {
            case PROCESS:
                nodeEdges = getEdgesFor((Process) entity);
                break;
            case FEED:
                nodeEdges = getEdgesFor((Feed) entity);
                break;
        }
        if (nodeEdges == null) return;

        for (Map.Entry<Node, Set<Node>> entry : nodeEdges.entrySet()) {
            if (graph.containsKey(entry.getKey())) {
                graph.get(entry.getKey()).addAll(entry.getValue());
            } else {
                graph.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void onRemove(Entity entity) throws IvoryException {
        Map<Node, Set<Node>> nodeEdges = null;
        switch (entity.getEntityType()) {
            case PROCESS:
                nodeEdges = getEdgesFor((Process) entity);
                break;
            case FEED:
                nodeEdges = getEdgesFor((Feed) entity);
                break;
        }
        if (nodeEdges == null) return;

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
    public void onChange(Entity oldEntity, Entity newEntity)
            throws IvoryException {
        onRemove(oldEntity);
        onRemove(newEntity);
    }

    private Map<Node, Set<Node>> getEdgesFor(Process process) {
        Map<Node, Set<Node>> nodeEdges = new HashMap<Node, Set<Node>>();
        Node processNode = new Node(EntityType.PROCESS, process.getName());
        nodeEdges.put(processNode, new HashSet<Node>());
        Set<Node> processEdges = nodeEdges.get(processNode);
        if (process.getInputs() != null) {
            for (Input input : process.getInputs().getInput()) {
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
            for (Output output : process.getOutputs().getOutput()) {
                Node feedNode = new Node(EntityType.FEED, output.getFeed());
                if (!nodeEdges.containsKey(feedNode)) {
                    nodeEdges.put(feedNode, new HashSet<Node>());
                }
                Set<Node> feedEdges = nodeEdges.get(feedNode);
                processEdges.add(feedNode);
                feedEdges.add(processNode);
            }
        }
        Node clusterNode = new Node(EntityType.CLUSTER, process.getCluster().getName());
        processEdges.add(clusterNode);
        nodeEdges.put(clusterNode, new HashSet<Node>());
        nodeEdges.get(clusterNode).add(processNode);
        return nodeEdges;
    }

    private Map<Node, Set<Node>> getEdgesFor(Feed feed) {
        Map<Node, Set<Node>> nodeEdges = new HashMap<Node, Set<Node>>();
        Node feedNode = new Node(EntityType.FEED, feed.getName());
        Set<Node> feedEdges = new HashSet<Node>();
        nodeEdges.put(feedNode, feedEdges);

        for (org.apache.ivory.entity.v0.feed.Cluster cluster :
                feed.getClusters().getCluster()) {
            Node clusterNode = new Node(EntityType.CLUSTER, cluster.getName());
            if (!nodeEdges.containsKey(clusterNode)) {
                nodeEdges.put(clusterNode, new HashSet<Node>());
            }
            Set<Node> clusterEdges = nodeEdges.get(clusterNode);
            feedEdges.add(clusterNode);
            clusterEdges.add(feedNode);
        }
        return nodeEdges;
    }

    @Override
    public String getName() {
        return "graph";
    }

    @Override
    public void init() throws IvoryException {
    }

    @Override
    public void destroy() throws IvoryException {
    }

    private static class Node {

        private final EntityType type;
        private final String name;

        private Node(EntityType type, String name) {
            this.type = type;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Node node = (Node) o;

            boolean nameEqual = name != null ?
                    !name.equals(node.name) : node.name != null;

            if (nameEqual) return false;
            if (type != node.type) return false;

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
