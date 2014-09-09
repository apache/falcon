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

package org.apache.falcon.cluster.util;

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Clusters;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;

/**
 * Utility class to build entity objects.
 */
public final class EntityBuilderTestUtil {

    public static final String USER = System.getProperty("user.name");
    public static final String COLO_NAME = "west-coast";
    public static final String WORKFLOW_NAME = "imp-click-join-workflow";
    public static final String WORKFLOW_VERSION = "1.0.9";

    private EntityBuilderTestUtil() {
    }

    public static Cluster buildCluster(String name) {
        return buildCluster(name, COLO_NAME, "classification=production");
    }

    public static Cluster buildCluster(String name, String colo, String tags) {
        Cluster cluster = new Cluster();
        cluster.setName(name);
        cluster.setColo(colo);
        cluster.setTags(tags);

        Interfaces interfaces = new Interfaces();
        cluster.setInterfaces(interfaces);

        Interface storage = new Interface();
        storage.setEndpoint("jail://global:00");
        storage.setType(Interfacetype.WRITE);
        cluster.getInterfaces().getInterfaces().add(storage);

        org.apache.falcon.entity.v0.cluster.ACL clusterACL = new org.apache.falcon.entity.v0
                .cluster.ACL();
        clusterACL.setOwner(USER);
        clusterACL.setGroup(USER);
        clusterACL.setPermission("*");
        cluster.setACL(clusterACL);

        return cluster;
    }

    public static Feed buildFeed(String feedName, Cluster[] clusters, String tags, String groups) {
        Feed feed = new Feed();
        feed.setName(feedName);
        feed.setTags(tags);
        feed.setGroups(groups);
        feed.setFrequency(Frequency.fromString("hours(1)"));

        org.apache.falcon.entity.v0.feed.Clusters
                feedClusters = new org.apache.falcon.entity.v0.feed.Clusters();
        feed.setClusters(feedClusters);

        for (Cluster cluster : clusters) {
            org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                    new org.apache.falcon.entity.v0.feed.Cluster();
            feedCluster.setName(cluster.getName());
            feedClusters.getClusters().add(feedCluster);
        }

        org.apache.falcon.entity.v0.feed.ACL feedACL = new org.apache.falcon.entity.v0.feed.ACL();
        feedACL.setOwner(USER);
        feedACL.setGroup(USER);
        feedACL.setPermission("*");
        feed.setACL(feedACL);

        return feed;
    }

    public static Feed buildFeed(String feedName, Cluster cluster, String tags, String groups) {
        return buildFeed(feedName, new Cluster[]{cluster}, tags, groups);
    }

    public static org.apache.falcon.entity.v0.process.Process buildProcess(String processName,
                                                                           Cluster cluster,
                                                                           String tags) throws Exception {
        return buildProcess(processName, cluster, tags, null);
    }

    public static org.apache.falcon.entity.v0.process.Process buildProcess(String processName,
                                                                           Cluster cluster,
                                                                           String tags,
                                                                           String pipelineTags) throws Exception {
        org.apache.falcon.entity.v0.process.Process processEntity = new Process();
        processEntity.setName(processName);
        processEntity.setTags(tags);
        if (pipelineTags != null) {
            processEntity.setPipelines(pipelineTags);
        }

        org.apache.falcon.entity.v0.process.Cluster processCluster =
                new org.apache.falcon.entity.v0.process.Cluster();
        processCluster.setName(cluster.getName());
        processEntity.setClusters(new Clusters());
        processEntity.getClusters().getClusters().add(processCluster);

        addProcessACL(processEntity);

        return processEntity;
    }

    public static void addProcessWorkflow(Process process) {
        addProcessWorkflow(process, WORKFLOW_NAME, WORKFLOW_VERSION);
    }

    public static void addProcessWorkflow(Process process, String workflowName, String version) {
        Workflow workflow = new Workflow();
        workflow.setName(workflowName);
        workflow.setVersion(version);
        workflow.setEngine(EngineType.PIG);
        workflow.setPath("/falcon/test/workflow");

        process.setWorkflow(workflow);
    }

    public static void addProcessACL(Process processEntity) throws Exception {
        addProcessACL(processEntity, USER, USER);
    }

    public static void addProcessACL(Process processEntity, String user,
                                     String group) throws Exception {
        org.apache.falcon.entity.v0.process.ACL processACL = new org.apache.falcon.entity.v0.process.ACL();
        processACL.setOwner(user);
        processACL.setGroup(group);
        processACL.setPermission("*");
        processEntity.setACL(processACL);
    }

    public static void addInput(Process process, Feed feed) {
        if (process.getInputs() == null) {
            process.setInputs(new Inputs());
        }

        Inputs inputs = process.getInputs();
        Input input = new Input();
        input.setFeed(feed.getName());
        inputs.getInputs().add(input);
    }

    public static void addOutput(Process process, Feed feed) {
        if (process.getOutputs() == null) {
            process.setOutputs(new Outputs());
        }

        Outputs outputs = process.getOutputs();
        Output output = new Output();
        output.setFeed(feed.getName());
        outputs.getOutputs().add(output);
    }
}
