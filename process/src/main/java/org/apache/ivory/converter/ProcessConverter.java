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

package org.apache.ivory.converter;

import java.util.List;

import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.DATAIN;
import org.apache.ivory.oozie.coordinator.DATAOUT;
import org.apache.ivory.oozie.coordinator.DATASETS;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.dozer.DozerConverter;

public class ProcessConverter extends DozerConverter<Process, COORDINATORAPP> {

    private static final String EL_PREFIX = "ivory:";

    public ProcessConverter() {
        super(Process.class, COORDINATORAPP.class);
    }

    @Override
    public COORDINATORAPP convertTo(Process process, COORDINATORAPP coordinatorapp) {
        coordinatorapp.setFrequency("${coord:" + process.getFrequency() + "(" + process.getPeriodicity() + ")}");

        // instance EL expression mapping
        if ((coordinatorapp.getInputEvents() != null) && (coordinatorapp.getInputEvents().getDataIn() != null)) {
            for (DATAIN input : coordinatorapp.getInputEvents().getDataIn()) {
                input.setStartInstance(getELExpression(input.getStartInstance()));
                input.setEndInstance(getELExpression(input.getEndInstance()));
            }
        }
        if ((coordinatorapp.getOutputEvents() != null) && (coordinatorapp.getOutputEvents().getDataOut() != null)) {
            for (DATAOUT output : coordinatorapp.getOutputEvents().getDataOut()) {
                output.setInstance(getELExpression(output.getInstance()));
            }
        }

        //Add datasets
        String clusterName = process.getClusters().getCluster().get(0).getName();
        if(process.getInputs() != null && process.getInputs().getInput() != null) {            
            for(Input input:process.getInputs().getInput()) {
                SYNCDATASET syncdataset = createDataSet(input.getFeed(), input.getPartition(), clusterName);

                if(coordinatorapp.getDatasets() == null)
                    coordinatorapp.setDatasets(new DATASETS());
                coordinatorapp.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);
            }
        }

        if(process.getOutputs() != null && process.getOutputs().getOutput() != null) {
            for(Output output:process.getOutputs().getOutput()) {
                SYNCDATASET syncdataset = createDataSet(output.getFeed(), null, clusterName);

                if(coordinatorapp.getDatasets() == null)
                    coordinatorapp.setDatasets(new DATASETS());
                coordinatorapp.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);                
            }
        }
        return coordinatorapp;
    }
    
    private SYNCDATASET createDataSet(String feedName, String partition, String clusterName) {
        Feed feed;
        try {
            feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
        } catch (StoreAccessException e) {  
            throw new RuntimeException(e);
        }
        if(feed == null) //This should never happen as its checked in process validation
            throw new RuntimeException("Referenced feed " + feedName + " is not registered!");

        SYNCDATASET syncdataset = new SYNCDATASET();
        syncdataset.setName(feed.getName());
        if(partition == null)
            syncdataset.setUriTemplate("${nameNode}" + feed.getLocations().get(LocationType.DATA).getPath());
        else
            syncdataset.setUriTemplate("${nameNode}" + feed.getLocations().get(LocationType.DATA).getPath() + "/" + partition);
            
        syncdataset.setFrequency("${coord:" + feed.getFrequency() + "(" + feed.getPeriodicity() + ")}");
        
        Cluster cluster = getCluster(feed.getClusters().getCluster(), clusterName);
        syncdataset.setInitialInstance(cluster.getValidity().getStart());
        syncdataset.setTimezone(cluster.getValidity().getTimezone());
        // syncdataset.setDoneFlag("");
        return syncdataset;
    }

    private Cluster getCluster(List<Cluster> clusters, String clusterName) {
        if(clusters != null) {
            for(Cluster cluster:clusters)
                if(cluster.getName().equals(clusterName))
                    return cluster;
        }
        return null;
    }

    private String getELExpression(String expr) {
        if (expr != null) {
            expr = "${" + EL_PREFIX + expr + "}";
        }
        return expr;
    }

    @Override
    public Process convertFrom(COORDINATORAPP arg0, Process arg1) {
        // TODO Auto-generated method stub
        return null;
    }
}