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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Cluster;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.log4j.Logger;

import com.sun.tools.javac.util.Pair;

/**
 * Concrete Parser which has XML parsing and validation logic for Process XML.
 * 
 */
public class ProcessEntityParser extends EntityParser<Process> {

    private static final Logger LOG = Logger.getLogger(ProcessEntityParser.class);

    private static final String SCHEMA_FILE_NAME = "/schema/process/process-0.1.xsd";

    private void validateReferencedClusters(Feed feed, Set<String> processRefClusters) throws ValidationException {
        Set<String> feedRefclusters = new HashSet<String>();
        for (org.apache.ivory.entity.v0.feed.Cluster cluster : feed.getClusters().getCluster()) {
            feedRefclusters.add(cluster.getName());
        }
        if (!(feedRefclusters.containsAll(processRefClusters))) {
            throw new ValidationException("Dependent feed: " + feed.getName()
                    + " does not contain all the referenced clusters in the process");
        }
    }

    protected ProcessEntityParser() {
        super(EntityType.PROCESS, SCHEMA_FILE_NAME);
    }

    @Override
    public void validate(Process process) throws ValidationException, StoreAccessException {
        // check if dependent entities exists
        ConfigurationStore store = ConfigurationStore.get();
        List<Pair<EntityType, String>> entities = new ArrayList<Pair<EntityType,String>>();
        
        if(process.getClusters() == null || process.getClusters().getCluster() == null)
            throw new ValidationException("No clusters defined in process");
        
        Set<String> refClusters = new HashSet<String>();
        for (Cluster cluster : process.getClusters().getCluster()) {
            entities.add(new Pair<EntityType, String>(EntityType.CLUSTER, cluster.getName()));
            refClusters.add(cluster.getName());
        }
        validateEntitiesExist(entities);
        
        if(process.getInputs() != null && process.getInputs().getInput() != null)
            for (Input input : process.getInputs().getInput()) {
                validateEntityExists(EntityType.FEED, input.getFeed());
                validateReferencedClusters((Feed) store.get(EntityType.FEED, input.getFeed()), refClusters);
            }
        
        if(process.getOutputs() != null && process.getOutputs().getOutput() != null)
            for (Output output : process.getOutputs().getOutput()) {
                validateEntityExists(EntityType.FEED, output.getFeed());
                validateReferencedClusters((Feed) store.get(EntityType.FEED, output.getFeed()), refClusters);
            }        
        
        //validate partitions
        if(process.getInputs() != null && process.getInputs().getInput() != null)
            for (Input input : process.getInputs().getInput()) {
                if(input.getPartition() != null) {
                    String partition = input.getPartition();
                    String[] parts = partition.split("/");
                    Feed feed = ConfigurationStore.get().get(EntityType.FEED, input.getFeed());          
                    if(feed.getPartitions() == null || feed.getPartitions().getPartition() == null || 
                            feed.getPartitions().getPartition().size() == 0 || feed.getPartitions().getPartition().size() < parts.length)
                        throw new ValidationException("Partition specification in input " + input.getName() + " is wrong");
                }
            }
    }
}
