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

package org.apache.falcon.entity;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Validity;
import  org.apache.falcon.entity.v0.process.Sla;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.resource.SchedulableEntityInstance;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Helper methods for accessing process members.
 */
public final class ProcessHelper {

    private ProcessHelper() {}


    public static Cluster getCluster(Process process, String clusterName) {
        for (Cluster cluster : process.getClusters().getClusters()) {
            if (cluster.getName().equals(clusterName)) {
                return cluster;
            }
        }
        return null;
    }

    public static String getProcessWorkflowName(String workflowName, String processName) {
        return StringUtils.isEmpty(workflowName) ? processName + "-workflow" : workflowName;
    }

    public static Storage.TYPE getStorageType(org.apache.falcon.entity.v0.cluster.Cluster cluster,
                                              Process process) throws FalconException {
        Storage.TYPE storageType = Storage.TYPE.FILESYSTEM;
        if (process.getInputs() == null && process.getOutputs() == null) {
            return storageType;
        }

        if (process.getInputs() != null) {
            for (Input input : process.getInputs().getInputs()) {
                Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
                storageType = FeedHelper.getStorageType(feed, cluster);
                if (Storage.TYPE.TABLE == storageType) {
                    break;
                }
            }
        }

        // If input feeds storage type is file system check storage type of output feeds
        if (process.getOutputs() != null && Storage.TYPE.FILESYSTEM == storageType) {
            for (Output output : process.getOutputs().getOutputs()) {
                Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
                storageType = FeedHelper.getStorageType(feed, cluster);
                if (Storage.TYPE.TABLE == storageType) {
                    break;
                }
            }
        }

        return storageType;
    }

    private static void validateProcessInstance(Process process, Date instanceTime,
                                                org.apache.falcon.entity.v0.cluster.Cluster cluster) {
        //validate the cluster
        Cluster processCluster = getCluster(process, cluster.getName());
        if (processCluster == null) {
            throw new IllegalArgumentException("Cluster provided: " + cluster.getName()
                    + " is not a valid cluster for the process: " + process.getName());
        }

        // check if instanceTime is in validity range
        if (instanceTime.before(processCluster.getValidity().getStart())
                || !instanceTime.before(processCluster.getValidity().getEnd())) {
            throw new IllegalArgumentException("Instance time provided: " + instanceTime
                    + " is not in validity range of process: " + process.getName()
                    + "on cluster: " + cluster.getName());
        }

        // check instanceTime is valid on the basis of startTime and frequency
        Date nextInstance = EntityUtil.getNextStartTime(processCluster.getValidity().getStart(),
                process.getFrequency(), process.getTimezone(), instanceTime);
        if (!nextInstance.equals(instanceTime)) {
            throw new IllegalArgumentException("Instance time provided: " + instanceTime
                    + " for process: " + process.getName() + " is not a valid instance time on cluster: "
                    + cluster.getName() + " on the basis of startDate and frequency");
        }
    }

    /**
     * Given a process instance, returns the feed instances which are used as input for this process instance.
     *
     * @param process            given process
     * @param instanceTime       nominal time of the process instance
     * @param cluster            - cluster for the process instance
     * @param allowOptionalFeeds switch to indicate whether optional feeds should be considered in input feeds.
     * @return Set of input feed instances which are consumed by the given process instance.
     * @throws org.apache.falcon.FalconException
     */
    public static Set<SchedulableEntityInstance> getInputFeedInstances(Process process, Date instanceTime,
               org.apache.falcon.entity.v0.cluster.Cluster cluster, boolean allowOptionalFeeds) throws FalconException {

        // validate the inputs
        validateProcessInstance(process, instanceTime, cluster);

        Set<SchedulableEntityInstance> result = new HashSet<>();
        if (process.getInputs() != null) {
            ConfigurationStore store = ConfigurationStore.get();
            for (Input i : process.getInputs().getInputs()) {
                if (i.isOptional() && !allowOptionalFeeds) {
                    continue;
                }
                Feed feed = store.get(EntityType.FEED, i.getFeed());
                // inputStart is process instance time + (now - startTime)
                ExpressionHelper evaluator = ExpressionHelper.get();
                ExpressionHelper.setReferenceDate(instanceTime);
                Date inputInstanceStartDate = evaluator.evaluate(i.getStart(), Date.class);
                Date inputInstanceEndDate = evaluator.evaluate(i.getEnd(), Date.class);
                List<Date> instanceTimes = EntityUtil.getEntityInstanceTimes(feed, cluster.getName(),
                        inputInstanceStartDate, inputInstanceEndDate);
                SchedulableEntityInstance instance;
                for (Date time : instanceTimes) {
                    instance = new SchedulableEntityInstance(feed.getName(), cluster.getName(), time, EntityType.FEED);
                    instance.setTags(SchedulableEntityInstance.INPUT);
                    result.add(instance);
                }
            }
        }
        return result;
    }

    public static Set<SchedulableEntityInstance> getOutputFeedInstances(Process process, Date instanceTime,
                                        org.apache.falcon.entity.v0.cluster.Cluster cluster) throws FalconException {
        Set<SchedulableEntityInstance> result = new HashSet<>();

        // validate the inputs
        validateProcessInstance(process, instanceTime, cluster);

        if (process.getOutputs() != null && process.getOutputs().getOutputs() != null) {

            ExpressionHelper.setReferenceDate(instanceTime);
            ExpressionHelper evaluator = ExpressionHelper.get();
            SchedulableEntityInstance candidate;
            ConfigurationStore store = ConfigurationStore.get();
            for (Output output : process.getOutputs().getOutputs()) {

                Date outputInstance = evaluator.evaluate(output.getInstance(), Date.class);
                // find the feed
                Feed feed = store.get(EntityType.FEED, output.getFeed());
                org.apache.falcon.entity.v0.feed.Cluster fCluster = FeedHelper.getCluster(feed, cluster.getName());
                outputInstance = EntityUtil.getPreviousInstanceTime(fCluster.getValidity().getStart(),
                        feed.getFrequency(), feed.getTimezone(), outputInstance);
                candidate = new SchedulableEntityInstance(output.getFeed(), cluster.getName(), outputInstance,
                        EntityType.FEED);
                candidate.setTags(SchedulableEntityInstance.OUTPUT);
                result.add(candidate);
            }
        }
        return result;
    }

    public static Validity getClusterValidity(Process process, String clusterName) throws FalconException {
        org.apache.falcon.entity.v0.process.Cluster cluster = getCluster(process, clusterName);
        if (cluster == null) {
            throw new FalconException("Invalid cluster: " + clusterName + " for process: " + process.getName());
        }
        return cluster.getValidity();
    }

    public static Sla getSLA(String clusterName, Process process) throws FalconException{
        Cluster cluster = getCluster(process, clusterName);
        if (cluster == null){
            throw new FalconException("Invalid cluster: " + clusterName + " for process: " + process.getName());
        }
        return getSLA(cluster, process);
    }

    public static Sla getSLA(Cluster cluster, Process process) {
        final Sla clusterSla = cluster.getSla();
        if (clusterSla != null) {
            return clusterSla;
        }
        return process.getSla();
    }

    public static Date getProcessValidityStart(Process process, String clusterName) throws FalconException {
        Cluster processCluster = getCluster(process, clusterName);
        if (processCluster != null) {
            return processCluster.getValidity().getStart();
        } else {
            throw new FalconException("No matching cluster " + clusterName
                    + "found for process " + process.getName());
        }
    }
}
