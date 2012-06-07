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

package org.apache.ivory.entity.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ProcessHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Inputs;
import org.apache.ivory.entity.v0.process.LateInput;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Outputs;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.entity.v0.process.Validity;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Concrete Parser which has XML parsing and validation logic for Process XML.
 * 
 */
public class ProcessEntityParser extends EntityParser<Process> {

    public ProcessEntityParser() {
        super(EntityType.PROCESS);
    }

    @Override
    public void validate(Process process) throws IvoryException {
        // check if dependent entities exists
        for (org.apache.ivory.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
            String clusterName = cluster.getName();
            validateEntityExists(EntityType.CLUSTER, clusterName);
            validateProcessValidity(cluster.getValidity().getStart(), cluster.getValidity().getEnd());
            validateHDFSpaths(process, clusterName);

            if (process.getInputs() != null) {
                for (Input input : process.getInputs().getInputs()) {
                    validateEntityExists(EntityType.FEED, input.getFeed());
                    Feed feed = (Feed) ConfigurationStore.get().get(EntityType.FEED, input.getFeed());
                    CrossEntityValidations.validateFeedDefinedForCluster(feed, clusterName);
                    CrossEntityValidations.validateFeedRetentionPeriod(input.getStart(), feed, clusterName);
                    CrossEntityValidations.validateInstanceRange(process, input, feed);
                    if (input.getPartition() != null) {
                        CrossEntityValidations.validateInputPartition(input, feed);
                    }
                }
            }

            if (process.getOutputs() != null) {
                for (Output output : process.getOutputs().getOutputs()) {
                    validateEntityExists(EntityType.FEED, output.getFeed());
                    Feed feed = (Feed) ConfigurationStore.get().get(EntityType.FEED, output.getFeed());
                    CrossEntityValidations.validateFeedDefinedForCluster(feed, clusterName);
                    CrossEntityValidations.validateInstance(process, output, feed);
                }
            }
        }
        validateDatasetName(process.getInputs(), process.getOutputs());
        validateLateInputs(process);
    }

    private void validateHDFSpaths(Process process, String clusterName) throws IvoryException {
        org.apache.ivory.entity.v0.cluster.Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);
        String workflowPath = process.getWorkflow().getPath();
        String nameNode = getNameNode(cluster, clusterName);
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name", nameNode);
            FileSystem fs = FileSystem.get(configuration);
            if (!fs.exists(new Path(workflowPath))) {
                throw new ValidationException("Workflow path: " + workflowPath + " does not exists in HDFS: " + nameNode);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e);
        } catch (ConnectException e) {
            throw new ValidationException("Unable to connect to Namenode: " + nameNode + " referenced in cluster: " + clusterName);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    private String getNameNode(Cluster cluster, String clusterName) throws ValidationException {
        // cluster should never be null as it is validated while submitting
        // feeds.
        if (!ClusterHelper.getHdfsUrl(cluster).startsWith("hdfs://")) {
            throw new ValidationException("Cannot get valid nameNode from write interface of cluster: " + clusterName);
        }
        return ClusterHelper.getHdfsUrl(cluster);
    }

    private void validateProcessValidity(String start, String end) throws IvoryException {
        try {
            validateProcessDates(start, end);
            Date processStart = EntityUtil.parseDateUTC(start);
            Date processEnd = EntityUtil.parseDateUTC(end);
            if (!processStart.before(processEnd)) {
                throw new ValidationException("Process start time: " + start + " should be before process end time: " + end);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    private void validateProcessDates(String start, String end) throws ValidationException {
        if (!EntityUtil.isValidUTCDate(start)) {
            throw new ValidationException("Invalid start date: " + start);
        }
        if (!EntityUtil.isValidUTCDate(end)) {
            throw new ValidationException("Invalid end date: " + end);
        }
    }

    private void validateDatasetName(Inputs inputs, Outputs outputs) throws ValidationException {
        Set<String> datasetNames = new HashSet<String>();
        for (Input input : inputs.getInputs()) {
            if (!datasetNames.add(input.getName())) {
                throw new ValidationException("Input name: " + input.getName() + " is already used");
            }
        }
        for (Output output : outputs.getOutputs()) {
            if (!datasetNames.add(output.getName())) {
                throw new ValidationException("Output name: " + output.getName() + " is already used");
            }
        }
    }

    private void validateLateInputs(Process process) throws ValidationException {
        List<String> feedName = new ArrayList<String>();
        for (Input in : process.getInputs().getInputs()) {
            feedName.add(in.getName());
        }
        if (process.getLateProcess() != null) {
            for (LateInput lp : process.getLateProcess().getLateInputs()) {
                if (!feedName.contains(lp.getInput()))
                    throw new ValidationException("Late Input: " + lp.getInput() + " is not specified in the inputs");
            }
        }
    }
}
