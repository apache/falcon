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
import java.util.List;

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

    protected ProcessEntityParser() {
        super(EntityType.PROCESS, SCHEMA_FILE_NAME);
    }

    @Override
    protected void validate(Process process) throws ValidationException, StoreAccessException {
        // check if dependent entities exists
        List<Pair<EntityType, String>> entities = new ArrayList<Pair<EntityType,String>>();
        
        if(process.getClusters() == null || process.getClusters().getCluster() == null)
            throw new ValidationException("Wrong cluster specification");
        
        for (Cluster cluster : process.getClusters().getCluster()) {
            entities.add(new Pair<EntityType, String>(EntityType.CLUSTER, cluster.getName()));
        }
        
        if(process.getInputs() != null && process.getInputs().getInput() != null)
            for (Input input : process.getInputs().getInput()) {
                entities.add(new Pair<EntityType, String>(EntityType.FEED, input.getFeed()));
            }
        
        if(process.getOutputs() != null && process.getOutputs().getOutput() != null)
            for (Output output : process.getOutputs().getOutput()) {
                entities.add(new Pair<EntityType, String>(EntityType.FEED, output.getFeed()));
            }        
        validateEntitiesExist(entities);
        
        
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