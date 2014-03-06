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

package org.apache.falcon.metadata;

import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Entity Metadata relationship mapping helper.
 */
public class EntityRelationshipGraphBuilder extends RelationshipGraphBuilder {

    private static final Logger LOG = Logger.getLogger(EntityRelationshipGraphBuilder.class);

    // entity vertex types
    public static final String CLUSTER_ENTITY_TYPE = "cluster-entity";
    public static final String FEED_ENTITY_TYPE = "feed-entity";
    public static final String PROCESS_ENTITY_TYPE = "process-entity";


    public EntityRelationshipGraphBuilder(KeyIndexableGraph graph, boolean preserveHistory) {
        super(graph, preserveHistory);
    }

    public void addClusterEntity(Cluster clusterEntity) {
        LOG.info("Adding cluster entity: " + clusterEntity.getName());
        Vertex clusterVertex = addVertex(clusterEntity.getName(), CLUSTER_ENTITY_TYPE);

        // addUserRelation(clusterVertex, "created-by");  // audit info
        addColoRelation(clusterEntity.getColo(), clusterVertex);
        addDataClassification(clusterEntity.getTags(), clusterVertex);
    }

    public void addFeedEntity(Feed feed) {
        LOG.info("Adding feed entity: " + feed.getName());
        Vertex feedVertex = addVertex(feed.getName(), FEED_ENTITY_TYPE);

        addUserRelation(feedVertex);
        addDataClassification(feed.getTags(), feedVertex);
        addGroups(feed.getGroups(), feedVertex);

        for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
            addRelationToCluster(feedVertex, feedCluster.getName(), FEED_CLUSTER_EDGE_LABEL);
        }
    }

    public void updateFeedEntity(Feed oldFeed, Feed newFeed) {
        LOG.info("Updating feed entity: " + newFeed.getName());
        Vertex feedEntityVertex = findVertex(oldFeed.getName(), FEED_ENTITY_TYPE);
        if (feedEntityVertex == null) {
            throw new IllegalStateException(oldFeed.getName() + " entity vertex must exist.");
        }

        updateDataClassification(oldFeed.getTags(), newFeed.getTags(), feedEntityVertex);
        updateGroups(oldFeed.getGroups(), newFeed.getGroups(), feedEntityVertex);
        updateFeedClusters(oldFeed.getClusters().getClusters(),
                newFeed.getClusters().getClusters(), feedEntityVertex);
    }

    public void addProcessEntity(Process process) {
        String processName = process.getName();
        LOG.info("Adding process entity: " + processName);
        Vertex processVertex = addVertex(processName, PROCESS_ENTITY_TYPE);
        addWorkflowProperties(process.getWorkflow(), processVertex, processName);

        addUserRelation(processVertex);
        addDataClassification(process.getTags(), processVertex);

        for (org.apache.falcon.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
            addRelationToCluster(processVertex, cluster.getName(), PROCESS_CLUSTER_EDGE_LABEL);
        }

        addInputFeeds(process.getInputs(), processVertex);
        addOutputFeeds(process.getOutputs(), processVertex);
        // addWorkflow(process.getWorkflow(), processVertex, processName);
    }

    public void updateProcessEntity(Process oldProcess, Process newProcess) {
        LOG.info("Updating process entity: " + newProcess.getName());
        Vertex processEntityVertex = findVertex(oldProcess.getName(), PROCESS_ENTITY_TYPE);
        if (processEntityVertex == null) {
            throw new IllegalStateException(oldProcess.getName() + " entity vertex must exist.");
        }

        updateWorkflowProperties(oldProcess.getWorkflow(), newProcess.getWorkflow(),
                processEntityVertex, newProcess.getName());
        updateDataClassification(oldProcess.getTags(), newProcess.getTags(), processEntityVertex);
        updateProcessClusters(oldProcess.getClusters().getClusters(),
                newProcess.getClusters().getClusters(), processEntityVertex);
        updateProcessInputs(oldProcess.getInputs(), newProcess.getInputs(), processEntityVertex);
        updateProcessOutputs(oldProcess.getOutputs(), newProcess.getOutputs(), processEntityVertex);
    }

    public void addColoRelation(String colo, Vertex fromVertex) {
        Vertex coloVertex = addVertex(colo, COLO_TYPE);
        addEdge(fromVertex, coloVertex, CLUSTER_COLO_LABEL);
    }

    public void addRelationToCluster(Vertex fromVertex, String clusterName, String edgeLabel) {
        Vertex clusterVertex = findVertex(clusterName, CLUSTER_ENTITY_TYPE);
        if (clusterVertex == null) { // cluster must exist before adding other entities
            throw new IllegalStateException("Cluster entity vertex must exist: " + clusterName);
        }

        addEdge(fromVertex, clusterVertex, edgeLabel);
    }

    public void addInputFeeds(Inputs inputs, Vertex processVertex) {
        if (inputs == null) {
            return;
        }

        for (Input input : inputs.getInputs()) {
            addProcessFeedEdge(processVertex, input.getFeed(), FEED_PROCESS_EDGE_LABEL);
        }
    }

    public void addOutputFeeds(Outputs outputs, Vertex processVertex) {
        if (outputs == null) {
            return;
        }

        for (Output output : outputs.getOutputs()) {
            addProcessFeedEdge(processVertex, output.getFeed(), PROCESS_FEED_EDGE_LABEL);
        }
    }

    public void addProcessFeedEdge(Vertex processVertex, String feedName, String edgeLabel) {
        Vertex feedVertex = findVertex(feedName, FEED_ENTITY_TYPE);
        if (feedVertex == null) {
            throw new IllegalStateException("Feed entity vertex must exist: " + feedName);
        }

        addProcessFeedEdge(processVertex, feedVertex, edgeLabel);
    }

    public void addWorkflowProperties(Workflow workflow, Vertex processVertex, String processName) {
        processVertex.setProperty(LineageArgs.USER_WORKFLOW_NAME.getOptionName(),
                ProcessHelper.getProcessWorkflowName(workflow.getName(), processName));
        processVertex.setProperty(VERSION_PROPERTY_KEY, workflow.getVersion());
        processVertex.setProperty(LineageArgs.USER_WORKFLOW_ENGINE.getOptionName(), workflow.getEngine().value());
    }

    public void updateWorkflowProperties(Workflow oldWorkflow, Workflow newWorkflow,
                                         Vertex processEntityVertex, String processName) {
        if (areSame(oldWorkflow, newWorkflow)) {
            return;
        }

        LOG.info("Updating workflow properties for: " + processEntityVertex);
        addWorkflowProperties(newWorkflow, processEntityVertex, processName);
    }

    public void updateDataClassification(String oldClassification, String newClassification,
                                         Vertex entityVertex) {
        if (areSame(oldClassification, newClassification)) {
            return;
        }

        removeDataClassification(oldClassification, entityVertex);
        addDataClassification(newClassification, entityVertex);
    }

    private void removeDataClassification(String classification, Vertex entityVertex) {
        if (classification == null || classification.length() == 0) {
            return;
        }

        String[] oldTags = classification.split(",");
        for (String oldTag : oldTags) {
            int index = oldTag.indexOf("=");
            String tagKey = oldTag.substring(0, index);
            String tagValue = oldTag.substring(index + 1, oldTag.length());

            removeEdge(entityVertex, tagValue, tagKey);
        }
    }

    public void updateGroups(String oldGroups, String newGroups, Vertex entityVertex) {
        if (areSame(oldGroups, newGroups)) {
            return;
        }

        removeGroups(oldGroups, entityVertex);
        addGroups(newGroups, entityVertex);
    }

    private void removeGroups(String groups, Vertex entityVertex) {
        if (groups == null || groups.length() == 0) {
            return;
        }

        String[] oldGroupTags = groups.split(",");
        for (String groupTag : oldGroupTags) {
            removeEdge(entityVertex, groupTag, RelationshipGraphBuilder.GROUPS_LABEL);
        }
    }

    public static boolean areSame(String oldValue, String newValue) {
        return oldValue == null && newValue == null
                || oldValue != null && newValue != null && oldValue.equals(newValue);
    }

    public void updateFeedClusters(List<org.apache.falcon.entity.v0.feed.Cluster> oldClusters,
                                   List<org.apache.falcon.entity.v0.feed.Cluster> newClusters,
                                   Vertex feedEntityVertex) {
        if (areFeedClustersSame(oldClusters, newClusters)) {
            return;
        }

        // remove edges to old clusters
        for (org.apache.falcon.entity.v0.feed.Cluster oldCuster : oldClusters) {
            removeEdge(feedEntityVertex, oldCuster.getName(), FEED_CLUSTER_EDGE_LABEL);
        }

        // add edges to new clusters
        for (org.apache.falcon.entity.v0.feed.Cluster newCluster : newClusters) {
            addRelationToCluster(feedEntityVertex, newCluster.getName(), FEED_CLUSTER_EDGE_LABEL);
        }
    }

    public boolean areFeedClustersSame(List<org.apache.falcon.entity.v0.feed.Cluster> oldClusters,
                                       List<org.apache.falcon.entity.v0.feed.Cluster> newClusters) {
        if (oldClusters.size() != newClusters.size()) {
            return false;
        }

        List<String> oldClusterNames = getFeedClusterNames(oldClusters);
        List<String> newClusterNames = getFeedClusterNames(newClusters);

        return oldClusterNames.size() == newClusterNames.size()
                && oldClusterNames.containsAll(newClusterNames)
                && newClusterNames.containsAll(oldClusterNames);
    }

    public List<String> getFeedClusterNames(List<org.apache.falcon.entity.v0.feed.Cluster> clusters) {
        List<String> clusterNames = new ArrayList<String>(clusters.size());
        for (org.apache.falcon.entity.v0.feed.Cluster cluster : clusters) {
            clusterNames.add(cluster.getName());
        }

        return clusterNames;
    }

    public void updateProcessClusters(List<org.apache.falcon.entity.v0.process.Cluster> oldClusters,
                                      List<org.apache.falcon.entity.v0.process.Cluster> newClusters,
                                      Vertex processEntityVertex) {
        if (areProcessClustersSame(oldClusters, newClusters)) {
            return;
        }

        // remove old clusters
        for (org.apache.falcon.entity.v0.process.Cluster oldCuster : oldClusters) {
            removeEdge(processEntityVertex, oldCuster.getName(), PROCESS_CLUSTER_EDGE_LABEL);
        }

        // add new clusters
        for (org.apache.falcon.entity.v0.process.Cluster newCluster : newClusters) {
            addRelationToCluster(processEntityVertex, newCluster.getName(), PROCESS_CLUSTER_EDGE_LABEL);
        }
    }

    public boolean areProcessClustersSame(List<org.apache.falcon.entity.v0.process.Cluster> oldClusters,
                                          List<org.apache.falcon.entity.v0.process.Cluster> newClusters) {
        if (oldClusters.size() != newClusters.size()) {
            return false;
        }

        List<String> oldClusterNames = getProcessClusterNames(oldClusters);
        List<String> newClusterNames = getProcessClusterNames(newClusters);

        return oldClusterNames.size() == newClusterNames.size()
                && oldClusterNames.containsAll(newClusterNames)
                && newClusterNames.containsAll(oldClusterNames);
    }

    public List<String> getProcessClusterNames(List<org.apache.falcon.entity.v0.process.Cluster> clusters) {
        List<String> clusterNames = new ArrayList<String>(clusters.size());
        for (org.apache.falcon.entity.v0.process.Cluster cluster : clusters) {
            clusterNames.add(cluster.getName());
        }

        return clusterNames;
    }

    public static boolean areSame(Workflow oldWorkflow, Workflow newWorkflow) {
        return areSame(oldWorkflow.getName(), newWorkflow.getName())
                && areSame(oldWorkflow.getVersion(), newWorkflow.getVersion())
                && areSame(oldWorkflow.getEngine().value(), newWorkflow.getEngine().value());
    }

    private void updateProcessInputs(Inputs oldProcessInputs, Inputs newProcessInputs,
                                     Vertex processEntityVertex) {
        if (areSame(oldProcessInputs, newProcessInputs)) {
            return;
        }

        removeInputFeeds(oldProcessInputs, processEntityVertex);
        addInputFeeds(newProcessInputs, processEntityVertex);
    }

    public static boolean areSame(Inputs oldProcessInputs, Inputs newProcessInputs) {
        if (oldProcessInputs == null && newProcessInputs == null
                || oldProcessInputs == null || newProcessInputs == null
                || oldProcessInputs.getInputs().size() != newProcessInputs.getInputs().size()) {
            return false;
        }

        List<Input> oldInputs = oldProcessInputs.getInputs();
        List<Input> newInputs = newProcessInputs.getInputs();

        return oldInputs.size() == newInputs.size()
                && oldInputs.containsAll(newInputs)
                && newInputs.containsAll(oldInputs);
    }

    public void removeInputFeeds(Inputs inputs, Vertex processVertex) {
        if (inputs == null) {
            return;
        }

        for (Input input : inputs.getInputs()) {
            removeProcessFeedEdge(processVertex, input.getFeed(), FEED_PROCESS_EDGE_LABEL);
        }
    }

    public void removeOutputFeeds(Outputs outputs, Vertex processVertex) {
        if (outputs == null) {
            return;
        }

        for (Output output : outputs.getOutputs()) {
            removeProcessFeedEdge(processVertex, output.getFeed(), PROCESS_FEED_EDGE_LABEL);
        }
    }

    public void removeProcessFeedEdge(Vertex processVertex, String feedName, String edgeLabel) {
        Vertex feedVertex = findVertex(feedName, FEED_ENTITY_TYPE);
        if (feedVertex == null) {
            throw new IllegalStateException("Feed entity vertex must exist: " + feedName);
        }

        if (edgeLabel.equals(FEED_PROCESS_EDGE_LABEL)) {
            removeEdge(feedVertex, processVertex, edgeLabel);
        } else {
            removeEdge(processVertex, feedVertex, edgeLabel);
        }
    }

    private void updateProcessOutputs(Outputs oldProcessOutputs, Outputs newProcessOutputs,
                                      Vertex processEntityVertex) {
        if (areSame(oldProcessOutputs, newProcessOutputs)) {
            return;
        }

        removeOutputFeeds(oldProcessOutputs, processEntityVertex);
        addOutputFeeds(newProcessOutputs, processEntityVertex);
    }

    public static boolean areSame(Outputs oldProcessOutputs, Outputs newProcessOutputs) {
        if (oldProcessOutputs == null && newProcessOutputs == null
                || oldProcessOutputs == null || newProcessOutputs == null
                || oldProcessOutputs.getOutputs().size() != newProcessOutputs.getOutputs().size()) {
            return false;
        }

        List<Output> oldOutputs = oldProcessOutputs.getOutputs();
        List<Output> newOutputs = newProcessOutputs.getOutputs();

        return oldOutputs.size() == newOutputs.size()
                && oldOutputs.containsAll(newOutputs)
                && newOutputs.containsAll(oldOutputs);
    }
}
