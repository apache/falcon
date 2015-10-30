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
package org.apache.falcon.unit;

import org.apache.falcon.LifeCycle;
import org.apache.falcon.resource.AbstractInstanceManager;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;

import java.util.List;
import java.util.Properties;

/**
 * A proxy implementation of the entity instance operations.
 */
public class LocalInstanceManager extends AbstractInstanceManager {

    public LocalInstanceManager() {}

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public InstancesResult killInstance(Properties properties, String type, String entity, String startStr,
                                        String endStr, String colo, List<LifeCycle> lifeCycles) {
        return super.killInstance(properties, type, entity, startStr, endStr, colo, lifeCycles);
    }

    public InstancesResult suspendInstance(Properties properties, String type, String entity, String startStr,
                                           String endStr, String colo, List<LifeCycle> lifeCycles) {
        return super.suspendInstance(properties, type, entity, startStr, endStr, colo, lifeCycles);
    }

    public InstancesResult resumeInstance(Properties properties, String type, String entity, String startStr,
                                          String endStr, String colo, List<LifeCycle> lifeCycles) {
        return super.resumeInstance(properties, type, entity, startStr, endStr, colo, lifeCycles);
    }

    public InstancesResult reRunInstance(String type, String entity, String startStr, String endStr,
                                         Properties properties, String colo, List<LifeCycle> lifeCycles,
                                         Boolean isForced) {
        return super.reRunInstance(type, entity, startStr, endStr, properties, colo, lifeCycles, isForced);
    }

    public InstancesResult getStatusOfInstances(String type, String entity, String start, String end,
                                                String colo, List<LifeCycle> lifeCycles, String filterBy,
                                                String orderBy, String sortOrder, Integer offset,
                                                Integer numResults) {
        return super.getStatus(type, entity, start, end, colo, lifeCycles, filterBy, orderBy, sortOrder,
                offset, numResults);
    }

    public InstancesSummaryResult getSummary(String type, String entity, String startStr, String endStr, String colo,
                                             List<LifeCycle> lifeCycles, String filterBy, String orderBy,
                                             String sortOrder) {
        return super.getSummary(type, entity, startStr, endStr, colo, lifeCycles, filterBy, orderBy, sortOrder);
    }

    public FeedInstanceResult getListing(String type, String entity, String startStr, String endStr, String colo) {
        return super.getListing(type, entity, startStr, endStr, colo);
    }

    public InstancesResult getLogs(String type, String entity, String startStr, String endStr, String colo,
                                   String runId, List<LifeCycle> lifeCycles, String filterBy, String orderBy,
                                   String sortOrder, Integer offset, Integer numResults) {
        return super.getLogs(type, entity, startStr, endStr, colo, runId, lifeCycles, filterBy, orderBy, sortOrder,
                offset, numResults);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    public InstancesResult getInstanceParams(String type, String entity, String startTime, String colo,
                                             List<LifeCycle> lifeCycles) {
        return super.getInstanceParams(type, entity, startTime, colo, lifeCycles);
    }

    public InstanceDependencyResult getInstanceDependencies(String entityType, String entityName,
                                                            String instanceTimeString, String colo) {
        return super.getInstanceDependencies(entityType, entityName, instanceTimeString, colo);
    }
}
