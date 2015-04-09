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

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Entity Metadata relationship mapping helper.
 */
public class EntityRelationshipGraphBuilder extends RelationshipGraphBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(EntityRelationshipGraphBuilder.class);


    public EntityRelationshipGraphBuilder(Graph graph, boolean preserveHistory) {
        super(graph, preserveHistory);
    }

    public void addClusterEntity(Cluster clusterEntity) {
        LOG.info("Adding cluster entity: {}", clusterEntity.getName());
        Vertex clusterVertex = addVertex(clusterEntity.getName(), RelationshipType.CLUSTER_ENTITY);

        addColoRelation(clusterEntity.getColo(), clusterVertex);
        addDataClassification(clusterEntity.getTags(), clusterVertex);
    }

    public void addFeedEntity(Feed feed) {
        LOG.info("Adding feed entity: {}", feed.getName());
        Vertex feedVertex = addVertex(feed.getName(), RelationshipType.FEED_ENTITY);

        addUserRelation(feedVertex);
        addDataClassification(feed.getTags(), feedVertex);
        addGroups(feed.getGroups(), feedVertex);

        for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
            if (ClusterType.TARGET != feedCluster.getType()) {
                addRelationToCluster(feedVertex, feedCluster.getName(), RelationshipLabel.FEED_CLUSTER_EDGE);
            }
        }
    }

    public void updateFeedEntity(Feed oldFeed, Feed newFeed) {
        LOG.info("Updating feed entity: {}", newFeed.getName());
        Vertex feedEntityVertex = findVertex(oldFeed.getName(), RelationshipType.FEED_ENTITY);
        if (feedEntityVertex == null) {
            // todo - throw new IllegalStateException(oldFeed.getName() + " entity vertex must exist.");
            LOG.error("Illegal State: Feed entity vertex must exist for {}", oldFeed.getName());
            return;
        }

        updateDataClassification(oldFeed.getTags(), newFeed.getTags(), feedEntityVertex);
        updateGroups(oldFeed.getGroups(), newFeed.getGroups(), feedEntityVertex);
        updateFeedClusters(oldFeed.getClusters().getClusters(),
                newFeed.getClusters().getClusters(), feedEntityVertex);
    }

    public void addProcessEntity(Process process) {
        String processName = process.getName();
        LOG.info("Adding process entity: {}", processName);
        Vertex processVertex = addVertex(processName, RelationshipType.PROCESS_ENTITY);
        addWorkflowProperties(process.getWorkflow(), processVertex, processName);

        addUserRelation(processVertex);
        addDataClassification(process.getTags(), processVertex);
        addPipelines(process.getPipelines(), processVertex);

        for (org.apache.falcon.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
            addRelationToCluster(processVertex, cluster.getName(), RelationshipLabel.PROCESS_CLUSTER_EDGE);
        }

        addInputFeeds(process.getInputs(), processVertex);
        addOutputFeeds(process.getOutputs(), processVertex);
    }

    public void updateProcessEntity(Process oldProcess, Process newProcess) {
        LOG.info("Updating process entity: {}", newProcess.getName());
        Vertex processEntityVertex = findVertex(oldProcess.getName(), RelationshipType.PROCESS_ENTITY);
        if (processEntityVertex == null) {
            // todo - throw new IllegalStateException(oldProcess.getName() + " entity vertex must exist");
            LOG.error("Illegal State: Process entity vertex must exist for {}", oldProcess.getName());
            return;
        }

        updateWorkflowProperties(oldProcess.getWorkflow(), newProcess.getWorkflow(),
                processEntityVertex, newProcess.getName());
        updateDataClassification(oldProcess.getTags(), newProcess.getTags(), processEntityVertex);
        updatePipelines(oldProcess.getPipelines(), newProcess.getPipelines(), processEntityVertex);
        updateProcessClusters(oldProcess.getClusters().getClusters(),
                newProcess.getClusters().getClusters(), processEntityVertex);
        updateProcessInputs(oldProcess.getInputs(), newProcess.getInputs(), processEntityVertex);
        updateProcessOutputs(oldProcess.getOutputs(), newProcess.getOutputs(), processEntityVertex);
    }

    public void addColoRelation(String colo, Vertex fromVertex) {
        Vertex coloVertex = addVertex(colo, RelationshipType.COLO);
        addEdge(fromVertex, coloVertex, RelationshipLabel.CLUSTER_COLO.getName());
    }

    public void addRelationToCluster(Vertex fromVertex, String clusterName, RelationshipLabel edgeLabel) {
        Vertex clusterVertex = findVertex(clusterName, RelationshipType.CLUSTER_ENTITY);
        if (clusterVertex == null) { // cluster must exist before adding other entities
            // todo - throw new IllegalStateException("Cluster entity vertex must exist: " + clusterName);
            LOG.error("Illegal State: Cluster entity vertex must exist for {}", clusterName);
            return;
        }

        addEdge(fromVertex, clusterVertex, edgeLabel.getName());
    }

    public void addInputFeeds(Inputs inputs, Vertex processVertex) {
        if (inputs == null) {
            return;
        }

        for (Input input : inputs.getInputs()) {
            addProcessFeedEdge(processVertex, input.getFeed(), RelationshipLabel.FEED_PROCESS_EDGE);
        }
    }

    public void addOutputFeeds(Outputs outputs, Vertex processVertex) {
        if (outputs == null) {
            return;
        }

        for (Output output : outputs.getOutputs()) {
            addProcessFeedEdge(processVertex, output.getFeed(), RelationshipLabel.PROCESS_FEED_EDGE);
        }
    }

    public void addProcessFeedEdge(Vertex processVertex, String feedName, RelationshipLabel edgeLabel) {
        Vertex feedVertex = findVertex(feedName, RelationshipType.FEED_ENTITY);
        if (feedVertex == null) {
            // todo - throw new IllegalStateException("Feed entity vertex must exist: " + feedName);
            LOG.error("Illegal State: Feed entity vertex must exist for {}", feedName);
            return;
        }

        addProcessFeedEdge(processVertex, feedVertex, edgeLabel);
    }

    public void addWorkflowProperties(Workflow workflow, Vertex processVertex, String processName) {
        processVertex.setProperty(WorkflowExecutionArgs.USER_WORKFLOW_NAME.getName(),
                ProcessHelper.getProcessWorkflowName(workflow.getName(), processName));
        processVertex.setProperty(RelationshipProperty.VERSION.getName(), workflow.getVersion());
        processVertex.setProperty(WorkflowExecutionArgs.USER_WORKFLOW_ENGINE.getName(),
                workflow.getEngine().value());
    }

    public void updateWorkflowProperties(Workflow oldWorkflow, Workflow newWorkflow,
                                         Vertex processEntityVertex, String processName) {
        if (areSame(oldWorkflow, newWorkflow)) {
            return;
        }

        LOG.info("Updating workflow properties for: {}", processEntityVertex);
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

    public void updatePipelines(String oldPipelines, String newPipelines, Vertex entityVertex) {
        if (areSame(oldPipelines, newPipelines)) {
            return;
        }

        removePipelines(oldPipelines, entityVertex);
        addPipelines(newPipelines, entityVertex);
    }

    private void removeGroups(String groups, Vertex entityVertex) {
        removeGroupsOrPipelines(groups, entityVertex, RelationshipLabel.GROUPS);
    }

    private void removePipelines(String pipelines, Vertex entityVertex) {
        removeGroupsOrPipelines(pipelines, entityVertex, RelationshipLabel.PIPELINES);
    }

    private void removeGroupsOrPipelines(String groupsOrPipelines, Vertex entityVertex,
                                         RelationshipLabel edgeLabel) {
        if (StringUtils.isEmpty(groupsOrPipelines)) {
            return;
        }

        String[] oldGroupOrPipelinesTags = groupsOrPipelines.split(",");
        for (String groupOrPipelineTag : oldGroupOrPipelinesTags) {
            removeEdge(entityVertex, groupOrPipelineTag, edgeLabel.getName());
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
            if (ClusterType.TARGET != oldCuster.getType()) {
                removeEdge(feedEntityVertex, oldCuster.getName(),
                        RelationshipLabel.FEED_CLUSTER_EDGE.getName());
            }
        }

        // add edges to new clusters
        for (org.apache.falcon.entity.v0.feed.Cluster newCluster : newClusters) {
            if (ClusterType.TARGET != newCluster.getType()) {
                addRelationToCluster(feedEntityVertex, newCluster.getName(),
                        RelationshipLabel.FEED_CLUSTER_EDGE);
            }
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
            removeEdge(processEntityVertex, oldCuster.getName(),
                    RelationshipLabel.PROCESS_CLUSTER_EDGE.getName());
        }

        // add new clusters
        for (org.apache.falcon.entity.v0.process.Cluster newCluster : newClusters) {
            addRelationToCluster(processEntityVertex, newCluster.getName(),
                    RelationshipLabel.PROCESS_CLUSTER_EDGE);
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
        if (oldProcessInputs == null && newProcessInputs == null) {
            return true;
        }

        if (oldProcessInputs == null || newProcessInputs == null
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
            removeProcessFeedEdge(processVertex, input.getFeed(), RelationshipLabel.FEED_PROCESS_EDGE);
        }
    }

    public void removeOutputFeeds(Outputs outputs, Vertex processVertex) {
        if (outputs == null) {
            return;
        }

        for (Output output : outputs.getOutputs()) {
            removeProcessFeedEdge(processVertex, output.getFeed(), RelationshipLabel.PROCESS_FEED_EDGE);
        }
    }

    public void removeProcessFeedEdge(Vertex processVertex, String feedName, RelationshipLabel edgeLabel) {
        Vertex feedVertex = findVertex(feedName, RelationshipType.FEED_ENTITY);
        if (feedVertex == null) {
            // todo - throw new IllegalStateException("Feed entity vertex must exist: " + feedName);
            LOG.error("Illegal State: Feed entity vertex must exist for {}", feedName);
            return;
        }

        if (edgeLabel == RelationshipLabel.FEED_PROCESS_EDGE) {
            removeEdge(feedVertex, processVertex, edgeLabel.getName());
        } else {
            removeEdge(processVertex, feedVertex, edgeLabel.getName());
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
        if (oldProcessOutputs == null && newProcessOutputs == null) {
            return true;
        }

        if (oldProcessOutputs == null || newProcessOutputs == null
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
