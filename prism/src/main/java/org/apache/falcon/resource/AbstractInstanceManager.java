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

package org.apache.falcon.resource;

import com.thinkaurelius.titan.core.TitanMultiVertexQuery;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.FeedInstanceStatus;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.logging.LogProvider;
import org.apache.falcon.metadata.GraphUtils;
import org.apache.falcon.metadata.RelationshipLabel;
import org.apache.falcon.metadata.RelationshipProperty;
import org.apache.falcon.metadata.RelationshipType;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesSummaryResult.InstanceSummary;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

/**
 * A base class for managing Entity's Instance operations.
 */
public abstract class AbstractInstanceManager extends AbstractEntityManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractInstanceManager.class);

    private static final long MINUTE_IN_MILLIS = 60000L;
    private static final long HOUR_IN_MILLIS = 3600000L;
    protected static final long DAY_IN_MILLIS = 86400000L;
    private static final long MONTH_IN_MILLIS = 2592000000L;

    protected EntityType checkType(String type) {
        if (StringUtils.isEmpty(type)) {
            throw FalconWebException.newAPIException("entity type is empty");
        } else {
            EntityType entityType = EntityType.getEnum(type);
            if (entityType == EntityType.CLUSTER) {
                throw FalconWebException.newAPIException(
                        "Instance management functions don't apply to Cluster entities");
            }
            return entityType;
        }
    }


    protected List<LifeCycle> checkAndUpdateLifeCycle(List<LifeCycle> lifeCycleValues,
                                                      String type) throws FalconException {
        EntityType entityType = EntityType.getEnum(type);
        if (lifeCycleValues == null || lifeCycleValues.isEmpty()) {
            List<LifeCycle> lifeCycles = new ArrayList<LifeCycle>();
            if (entityType == EntityType.PROCESS) {
                lifeCycles.add(LifeCycle.valueOf(LifeCycle.EXECUTION.name()));
            } else if (entityType == EntityType.FEED) {
                lifeCycles.add(LifeCycle.valueOf(LifeCycle.REPLICATION.name()));
            }
            return lifeCycles;
        }
        for (LifeCycle lifeCycle : lifeCycleValues) {
            if (entityType != lifeCycle.getTag().getType()) {
                throw new FalconException("Incorrect lifecycle: " + lifeCycle + "for given type: " + type);
            }
        }
        return lifeCycleValues;
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public InstancesResult getRunningInstances(String type, String entity,
                                               String colo, List<LifeCycle> lifeCycles, String filterBy,
                                               String orderBy, String sortOrder, Integer offset, Integer numResults) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateNotEmpty("entityName", entity);
            validateInstanceFilterByClause(filterBy);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine(entityObject);
            return getInstanceResultSubset(wfEngine.getRunningInstances(entityObject, lifeCycles),
                    filterBy, orderBy, sortOrder, offset, numResults, "");
        } catch (Throwable e) {
            LOG.error("Failed to get running instances", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    protected void validateInstanceFilterByClause(String entityFilterByClause) {
        Map<String, List<String>> filterByFieldsValues = getFilterByFieldsValues(entityFilterByClause);
        for (Map.Entry<String, List<String>> entry : filterByFieldsValues.entrySet()) {
            try {
                InstancesResult.InstanceFilterFields filterKey =
                        InstancesResult.InstanceFilterFields .valueOf(entry.getKey().toUpperCase());
                if (filterKey == InstancesResult.InstanceFilterFields.STARTEDAFTER) {
                    getEarliestDate(entry.getValue());
                }
            } catch (IllegalArgumentException e) {
                throw FalconWebException.newAPIException("Invalid filter key: " + entry.getKey());
            } catch (FalconException e) {
                throw FalconWebException.newAPIException("Invalid date value for key: " + entry.getKey());
            }
        }
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public InstancesResult getInstances(String type, String entity, String startStr, String endStr,
                                        String colo, List<LifeCycle> lifeCycles,
                                        String filterBy, String orderBy, String sortOrder,
                                        Integer offset, Integer numResults, Boolean allAttempts) {
        return getStatus(type, entity, startStr, endStr, colo, lifeCycles,
                filterBy, orderBy, sortOrder, offset, numResults, allAttempts);
    }

    public InstancesResult getStatus(String type, String entity, String startStr, String endStr,
                                     String colo, List<LifeCycle> lifeCycles,
                                     String filterBy, String orderBy, String sortOrder,
                                     Integer offset, Integer numResults, Boolean allAttempts) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            validateInstanceFilterByClause(filterBy);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDate(entityObject, startStr, endStr, numResults);

            // LifeCycle lifeCycleObject = EntityUtil.getLifeCycle(lifeCycle);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine(entityObject);
            return getInstanceResultSubset(wfEngine.getStatus(entityObject,
                    startAndEndDate.first, startAndEndDate.second, lifeCycles, allAttempts),
                    filterBy, orderBy, sortOrder, offset, numResults, startStr);
        } catch (FalconException e) {
            LOG.error("Failed to get instances status", e);
            throw FalconWebException.newAPIException(e.getMessage());
        }
    }


    public InstanceDependencyResult getInstanceDependencies(String entityType, String entityName,
                                                  String instanceTimeString, String colo) {
        checkColo(colo);
        EntityType type = checkType(entityType);
        Set<SchedulableEntityInstance> result = new HashSet<>();

        try {
            Date instanceTime = EntityUtil.parseDateUTC(instanceTimeString);
            for (String clusterName : DeploymentUtil.getCurrentClusters()) {
                Cluster cluster = EntityUtil.getEntity(EntityType.CLUSTER, clusterName);
                switch (type) {

                case PROCESS:
                    Process process = EntityUtil.getEntity(EntityType.PROCESS, entityName);
                    org.apache.falcon.entity.v0.process.Cluster pCluster = ProcessHelper.getCluster(process,
                            clusterName);
                    if (pCluster != null) {
                        Set<SchedulableEntityInstance> inputFeeds = ProcessHelper.getInputFeedInstances(process,
                                instanceTime, cluster, true);
                        Set<SchedulableEntityInstance> outputFeeds = ProcessHelper.getOutputFeedInstances(process,
                                instanceTime, cluster);
                        result.addAll(inputFeeds);
                        result.addAll(outputFeeds);
                    }
                    break;

                case FEED:
                    Feed feed = EntityUtil.getEntity(EntityType.FEED, entityName);
                    org.apache.falcon.entity.v0.feed.Cluster fCluster = FeedHelper.getCluster(feed, clusterName);
                    if (fCluster != null) {
                        Set<SchedulableEntityInstance> consumers = FeedHelper.getConsumerInstances(feed, instanceTime,
                                cluster);
                        SchedulableEntityInstance producer = FeedHelper.getProducerInstance(feed, instanceTime,
                                cluster);
                        result.addAll(consumers);
                        if (producer != null) {
                            result.add(producer);
                        }
                    }
                    break;

                default:
                    throw FalconWebException.newAPIException("Instance dependency isn't supported for type:"
                        + entityType);
                }
            }

        } catch (Throwable throwable) {
            LOG.error("Failed to get instance dependencies:", throwable);
            throw FalconWebException.newAPIException(throwable);
        }

        InstanceDependencyResult res = new InstanceDependencyResult(APIResult.Status.SUCCEEDED, "Success!");
        res.setDependencies(result.toArray(new SchedulableEntityInstance[0]));
        return res;
    }

    public InstancesSummaryResult getSummary(String type, String entity, String startStr, String endStr,
                                             String colo, List<LifeCycle> lifeCycles,
                                             String filterBy, String orderBy, String sortOrder) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDate(entityObject, startStr, endStr);

            AbstractWorkflowEngine wfEngine = getWorkflowEngine(entityObject);
            return getInstanceSummaryResultSubset(wfEngine.getSummary(entityObject,
                    startAndEndDate.first, startAndEndDate.second, lifeCycles),
                    filterBy, orderBy, sortOrder);

        } catch (Throwable e) {
            LOG.error("Failed to get instance summary", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    public InstancesResult getLogs(String type, String entity, String startStr, String endStr,
                                   String colo, String runId, List<LifeCycle> lifeCycles,
                                   String filterBy, String orderBy, String sortOrder,
                                   Integer offset, Integer numResults) {
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            // getStatus does all validations and filters clusters
            InstancesResult result = getStatus(type, entity, startStr, endStr,
                    colo, lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, null);
            LogProvider logProvider = new LogProvider();
            Entity entityObject = EntityUtil.getEntity(type, entity);
            for (Instance instance : result.getInstances()) {
                logProvider.populateLogUrls(entityObject, instance, runId);
            }
            return result;
        } catch (Exception e) {
            LOG.error("Failed to get logs for instances", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    //RESUME CHECKSTYLE CHECK ParameterNumberCheck
    private InstancesResult getInstanceResultSubset(InstancesResult resultSet, String filterBy,
                                                    String orderBy, String sortOrder, Integer offset,
                                                    Integer numResults, String startStr) throws FalconException {
        if (resultSet.getInstances() == null) {
            // return the empty resultSet
            resultSet.setInstances(new Instance[0]);
            return resultSet;
        }

        // Filter instances
        Map<String, List<String>> filterByFieldsValues = getFilterByFieldsValues(filterBy);
        List<Instance> instanceSet = getFilteredInstanceSet(resultSet, filterByFieldsValues);

        int pageCount = super.getRequiredNumberOfResults(instanceSet.size(), offset, numResults);
        InstancesResult result = new InstancesResult(resultSet.getStatus(), resultSet.getMessage());
        if (pageCount == 0) {
            // return empty result set
            result.setInstances(new Instance[0]);
            return result;
        }
        if (StringUtils.isNotEmpty(startStr) && StringUtils.isEmpty(sortOrder)) {
            Collections.reverse(instanceSet);
        }
        if (StringUtils.isNotEmpty(sortOrder)) {
            // Sort the ArrayList using orderBy
            instanceSet = sortInstances(instanceSet, orderBy.toLowerCase(), sortOrder);
        }
        result.setCollection(instanceSet.subList(
                offset, (offset + pageCount)).toArray(new Instance[pageCount]));
        return result;
    }

    private InstancesSummaryResult getInstanceSummaryResultSubset(InstancesSummaryResult resultSet, String filterBy,
                                                    String orderBy, String sortOrder) throws FalconException {
        if (resultSet.getInstancesSummary() == null) {
            // return the empty resultSet
            resultSet.setInstancesSummary(new InstancesSummaryResult.InstanceSummary[0]);
            return resultSet;
        }

        // Filter instances
        Map<String, List<String>> filterByFieldsValues = getFilterByFieldsValues(filterBy);
        List<InstanceSummary> instanceSet = getFilteredInstanceSummarySet(resultSet, filterByFieldsValues);

        InstancesSummaryResult result = new InstancesSummaryResult(resultSet.getStatus(), resultSet.getMessage());

        // Sort the ArrayList using orderBy
        instanceSet = sortInstanceSummary(instanceSet, orderBy.toLowerCase(), sortOrder);
        result.setInstancesSummary(instanceSet.toArray(new InstanceSummary[instanceSet.size()]));
        return result;
    }

    private List<Instance> getFilteredInstanceSet(InstancesResult resultSet,
                                                  Map<String, List<String>> filterByFieldsValues)
        throws FalconException {
        // If filterBy is empty, return all instances. Else return instances with matching filter.
        if (filterByFieldsValues.size() == 0) {
            return Arrays.asList(resultSet.getInstances());
        }

        List<Instance> instanceSet = new ArrayList<Instance>();
        for (Instance instance : resultSet.getInstances()) {   // for each instance
            boolean isInstanceFiltered = false;

            // for each filter
            for (Map.Entry<String, List<String>> pair : filterByFieldsValues.entrySet()) {
                if (isInstanceFiltered(instance, pair)) { // wait until all filters are applied
                    isInstanceFiltered = true;
                    break;  // no use to continue other filters as the current one filtered this
                }
            }

            if (!isInstanceFiltered) {  // survived all filters
                instanceSet.add(instance);
            }
        }

        return instanceSet;
    }

    private List<InstanceSummary> getFilteredInstanceSummarySet(InstancesSummaryResult resultSet,
                                                  Map<String, List<String>> filterByFieldsValues)
        throws FalconException {
        // If filterBy is empty, return all instances. Else return instances with matching filter.
        if (filterByFieldsValues.size() == 0) {
            return Arrays.asList(resultSet.getInstancesSummary());
        }

        List<InstanceSummary> instanceSet = new ArrayList<>();
        // for each instance
        for (InstanceSummary instance : resultSet.getInstancesSummary()) {
            // for each filter
            boolean isInstanceFiltered = false;
            Map<String, Long> newSummaryMap = null;
            for (Map.Entry<String, List<String>> pair : filterByFieldsValues.entrySet()) {
                switch (InstancesSummaryResult.InstanceSummaryFilterFields.valueOf(pair.getKey().toUpperCase())) {
                case CLUSTER:
                    if (instance.getCluster() == null || !containsIgnoreCase(pair.getValue(), instance.getCluster())) {
                        isInstanceFiltered = true;
                    }
                    break;

                case STATUS:
                    if (newSummaryMap == null) {
                        newSummaryMap = new HashMap<>();
                    }
                    if (instance.getSummaryMap() == null || instance.getSummaryMap().isEmpty()) {
                        isInstanceFiltered = true;
                    } else {
                        for (Map.Entry<String, Long> entry : instance.getSummaryMap().entrySet()) {
                            if (containsIgnoreCase(pair.getValue(), entry.getKey())) {
                                newSummaryMap.put(entry.getKey(), entry.getValue());
                            }
                        }

                    }
                    break;

                default:
                    isInstanceFiltered = true;
                }

                if (isInstanceFiltered) { // wait until all filters are applied
                    break;  // no use to continue other filters as the current one filtered this
                }
            }

            if (!isInstanceFiltered) {  // survived all filters
                instanceSet.add(new InstanceSummary(instance.getCluster(), newSummaryMap));
            }
        }

        return instanceSet;
    }

    private boolean isInstanceFiltered(Instance instance,
                                       Map.Entry<String, List<String>> pair) throws FalconException {
        final List<String> filterValue = pair.getValue();
        switch (InstancesResult.InstanceFilterFields.valueOf(pair.getKey().toUpperCase())) {
        case STATUS:
            return instance.getStatus() == null
                    || !containsIgnoreCase(filterValue, instance.getStatus().toString());

        case CLUSTER:
            return instance.getCluster() == null
                    || !containsIgnoreCase(filterValue, instance.getCluster());

        case SOURCECLUSTER:
            return instance.getSourceCluster() == null
                    || !containsIgnoreCase(filterValue, instance.getSourceCluster());

        case STARTEDAFTER:
            return instance.getStartTime() == null
                    || instance.getStartTime().before(getEarliestDate(filterValue));

        default:
            return true;
        }
    }

    private List<Instance> sortInstances(List<Instance> instanceSet,
                                         String orderBy, String sortOrder) {
        final String order = getValidSortOrder(sortOrder, orderBy);
        if (orderBy.equals("status")) {
            Collections.sort(instanceSet, new Comparator<Instance>() {
                @Override
                public int compare(Instance i1, Instance i2) {
                    if (i1.getStatus() == null) {
                        i1.status = InstancesResult.WorkflowStatus.ERROR;
                    }
                    if (i2.getStatus() == null) {
                        i2.status = InstancesResult.WorkflowStatus.ERROR;
                    }
                    return (order.equalsIgnoreCase("asc")) ? i1.getStatus().name().compareTo(i2.getStatus().name())
                            : i2.getStatus().name().compareTo(i1.getStatus().name());
                }
            });
        } else if (orderBy.equals("cluster")) {
            Collections.sort(instanceSet, new Comparator<Instance>() {
                @Override
                public int compare(Instance i1, Instance i2) {
                    return (order.equalsIgnoreCase("asc")) ? i1.getCluster().compareTo(i2.getCluster())
                            : i2.getCluster().compareTo(i1.getCluster());
                }
            });
        } else if (orderBy.equals("starttime")){
            Collections.sort(instanceSet, new Comparator<Instance>() {
                @Override
                public int compare(Instance i1, Instance i2) {
                    Date start1 = (i1.getStartTime() == null) ? new Date(0) : i1.getStartTime();
                    Date start2 = (i2.getStartTime() == null) ? new Date(0) : i2.getStartTime();
                    return (order.equalsIgnoreCase("asc")) ? start1.compareTo(start2)
                            : start2.compareTo(start1);
                }
            });
        } else if (orderBy.equals("endtime")) {
            Collections.sort(instanceSet, new Comparator<Instance>() {
                @Override
                public int compare(Instance i1, Instance i2) {
                    Date end1 = (i1.getEndTime() == null) ? new Date(0) : i1.getEndTime();
                    Date end2 = (i2.getEndTime() == null) ? new Date(0) : i2.getEndTime();
                    return (order.equalsIgnoreCase("asc")) ? end1.compareTo(end2)
                            : end2.compareTo(end1);
                }
            });
        }
        //Default : no sort
        return instanceSet;
    }

    private List<InstanceSummary> sortInstanceSummary(List<InstanceSummary> instanceSet,
                                         String orderBy, String sortOrder) {
        final String order = getValidSortOrder(sortOrder, orderBy);
        if (orderBy.equals("cluster")) {
            Collections.sort(instanceSet, new Comparator<InstanceSummary>() {
                @Override
                public int compare(InstanceSummary i1, InstanceSummary i2) {
                    return (order.equalsIgnoreCase("asc")) ? i1.getCluster().compareTo(i2.getCluster())
                            : i2.getCluster().compareTo(i1.getCluster());
                }
            });
        }//Default : no sort

        return instanceSet;
    }

    public FeedInstanceResult getListing(String type, String entity, String startStr,
                                         String endStr, String colo) {
        checkColo(colo);
        EntityType entityType = checkType(type);

        if (entityType != EntityType.FEED) {
            throw FalconWebException.newAPIException("getLocation is not applicable for " + entityType);
        }
        try {

            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDate(entityObject, startStr, endStr);

            return FeedHelper.getFeedInstanceListing(entityObject, startAndEndDate.first, startAndEndDate.second);
        } catch (FalconException e) {
            LOG.error("Failed to get instances listing", e);
            throw FalconWebException.newAPIException(e.getMessage());
        }
    }

    public InstancesResult getInstanceParams(String type,
                                          String entity, String startTime,
                                          String colo, List<LifeCycle> lifeCycles) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            if (lifeCycles.size() != 1) {
                throw new FalconException("For displaying wf-params there can't be more than one lifecycle "
                        + lifeCycles);
            }
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDate(entityObject, startTime, null);
            Date start = startAndEndDate.first;
            Date end = EntityUtil.getNextInstanceTime(start, EntityUtil.getFrequency(entityObject),
                    EntityUtil.getTimeZone(entityObject), 1);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine(entityObject);
            return wfEngine.getInstanceParams(entityObject, start, end, lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to display params of an instance", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    public InstancesResult killInstance(HttpServletRequest request, String type, String entity, String startStr,
                                        String endStr, String colo, List<LifeCycle> lifeCycles) {
        Properties props = getProperties(request);
        return killInstance(props, type, entity, startStr, endStr, colo, lifeCycles);
    }

    public InstancesResult killInstance(Properties props, String type, String entity, String startStr,
                                        String endStr, String colo, List<LifeCycle> lifeCycles) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDateForLifecycleOperations(
                    entityObject, startStr, endStr);

            AbstractWorkflowEngine wfEngine = getWorkflowEngine(entityObject);
            wfEngine.killInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles);
            return wfEngine.ignoreInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to kill instances", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    public InstancesResult suspendInstance(HttpServletRequest request, String type, String entity, String startStr,
                                           String endStr, String colo, List<LifeCycle> lifeCycles) {
        Properties props = getProperties(request);
        return suspendInstance(props, type, entity, startStr, endStr, colo, lifeCycles);
    }

    public InstancesResult suspendInstance(Properties props, String type, String entity, String startStr, String endStr,
                                           String colo, List<LifeCycle> lifeCycles) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDateForLifecycleOperations(
                    entityObject, startStr, endStr);

            AbstractWorkflowEngine wfEngine = getWorkflowEngine(entityObject);
            return wfEngine.suspendInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to suspend instances", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    public InstancesResult resumeInstance(HttpServletRequest request, String type, String entity, String startStr,
                                          String endStr, String colo,
                                          List<LifeCycle> lifeCycles) {
        Properties props = getProperties(request);
        return resumeInstance(props, type, entity, startStr, endStr, colo, lifeCycles);
    }

    public InstancesResult resumeInstance(Properties props, String type, String entity, String startStr, String endStr,
                                          String colo, List<LifeCycle> lifeCycles) {
        checkColo(colo);
        checkType(type);
        if (StartupProperties.isServerInSafeMode()) {
            throwSafemodeException("RESUME");
        }
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDateForLifecycleOperations(
                    entityObject, startStr, endStr);

            AbstractWorkflowEngine wfEngine = getWorkflowEngine(entityObject);
            return wfEngine.resumeInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to resume instances", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    /**
     * Triage method returns the graph of the ancestors in "UNSUCCESSFUL" state.
     *
     * It will traverse all the ancestor feed instances and process instances in the current instance's lineage.
     * It stops traversing a lineage line once it encounters a "SUCCESSFUL" instance as this feature is intended
     * to find the root cause of a pipeline failure.
     *
     * @param entityType type of the entity. Only feed and process are valid entity types for triage.
     * @param entityName name of the entity.
     * @param instanceTime time of the instance which should be used to triage.
     * @return Returns a list of ancestor entity instances which have failed.
     */
    public TriageResult triageInstance(String entityType, String entityName, String instanceTime, String colo) {

        checkColo(colo);
        checkType(entityType); // should be only process/feed
        checkName(entityName);
        try {
            EntityType type = EntityType.valueOf(entityType.toUpperCase());
            Entity entity = EntityUtil.getEntity(type, entityName);
            TriageResult result = new TriageResult(APIResult.Status.SUCCEEDED, "Success");
            List<LineageGraphResult> triageGraphs = new LinkedList<>();
            for (String clusterName : EntityUtil.getClustersDefinedInColos(entity)) {
                Cluster cluster = EntityUtil.getEntity(EntityType.CLUSTER, clusterName);
                triageGraphs.add(triage(type, entity, instanceTime, cluster));
            }
            LineageGraphResult[] triageGraphsArray = new LineageGraphResult[triageGraphs.size()];
            result.setTriageGraphs(triageGraphs.toArray(triageGraphsArray));
            return result;
        } catch (IllegalArgumentException e) { // bad entityType
            LOG.error("Bad Entity Type: {}", entityType);
            throw FalconWebException.newAPIException(e);
        } catch (EntityNotRegisteredException e) { // bad entityName
            LOG.error("Bad Entity Name : {}", entityName);
            throw FalconWebException.newAPIException(e);
        } catch (Throwable e) {
            LOG.error("Failed to triage", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    public InstancesResult searchInstances(String type, String nameSubsequence, String tagKeywords,
                                           String nominalStartTime, String nominalEndTime,
                                           String status, String orderBy, Integer offset, Integer resultsPerPage) {
        type = org.apache.commons.lang.StringUtils.isEmpty(type) ? "feed,process" : type;
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;

        // filter entities
        EntityList entityList = getEntityList(
                "", nameSubsequence, tagKeywords, type, "", "", "", "", 0, 0, "", true);

        // search instances with TitanDB
        TitanBlueprintsGraph titanGraph = (TitanBlueprintsGraph) getGraph();
        Map<TitanVertex, Iterable<TitanVertex>> instanceMap = titanInstances(
                titanGraph, entityList, resultsPerPage + offset, nominalStartTime, nominalEndTime, status, orderBy);

        // integrate search results from each entity
        List<Instance> instances = consolidateTitanInstances(instanceMap);

        // sort by descending order and pagination
        List<Instance> instancesReturn = sortInstancesPagination(instances, orderBy, "desc", offset, resultsPerPage);

        // output format
        InstancesResult result = new InstancesResult(APIResult.Status.SUCCEEDED, "Instances Search Results");
        result.setInstances(instancesReturn.toArray(new Instance[instancesReturn.size()]));
        titanGraph.commit();
        return result;
    }

    private Map<TitanVertex, Iterable<TitanVertex>> titanInstances(TitanBlueprintsGraph titanGraph,
                                                                   EntityList entityList, int numTopInstances,
                                                                   String nominalStartTime, String nominalEndTime,
                                                                   String status, String orderBy) {
        List<TitanVertex> entityVertices = new ArrayList<TitanVertex>();
        for (EntityList.EntityElement entityElement : entityList.getElements()) {
            String entityName = entityElement.name;
            String entityType = entityElement.type;
            RelationshipType relationshipType = RelationshipType.fromSchedulableEntityType(entityType);
            TitanVertex entityVertex = (TitanVertex) GraphUtils.findVertex(titanGraph, entityName, relationshipType);
            if (entityVertex == null) {
                LOG.warn("No entity vertex found for type " + entityType + ", name " + entityName);
            } else {
                entityVertices.add(entityVertex);
            }
        }

        if (entityVertices.isEmpty()) { // Need to add at least one vertex for TitanMultiVertexQuery
            return new HashMap<>();
        }

        TitanMultiVertexQuery vertexQuery = titanGraph.multiQuery(entityVertices)
                .labels(RelationshipLabel.INSTANCE_ENTITY_EDGE.getName());
        GraphUtils.addRangeQuery(vertexQuery, RelationshipProperty.NOMINAL_TIME, nominalStartTime, nominalEndTime);
        GraphUtils.addEqualityQuery(vertexQuery, RelationshipProperty.STATUS, status);
        GraphUtils.addOrderLimitQuery(vertexQuery, orderBy, numTopInstances);
        return vertexQuery.vertices();
    }

    private List<Instance> consolidateTitanInstances(Map<TitanVertex, Iterable<TitanVertex>> instanceMap) {
        List<Instance> instances = new ArrayList<>();
        for (Iterable<TitanVertex> vertices : instanceMap.values()) {
            for (TitanVertex vertex : vertices) {
                Instance instance = new Instance();
                instance.instance = vertex.getProperty(RelationshipProperty.NAME.getName());
                String instanceStatus = vertex.getProperty(RelationshipProperty.STATUS.getName());
                if (StringUtils.isNotEmpty(instanceStatus)) {
                    instance.status = InstancesResult.WorkflowStatus.valueOf(instanceStatus);
                }
                instances.add(instance);
            }
        }
        return instances;
    }

    protected List<Instance> sortInstancesPagination(List<Instance> instances, String orderBy, String sortOrder,
                                                     Integer offset, Integer resultsPerPage) {
        // sort instances
        instances = sortInstances(instances, orderBy, sortOrder);

        // pagination
        int pageCount = super.getRequiredNumberOfResults(instances.size(), offset, resultsPerPage);
        List<Instance> instancesReturn = new ArrayList<Instance>();
        if (pageCount > 0) {
            instancesReturn.addAll(instances.subList(offset, (offset + pageCount)));
        }
        return instancesReturn;
    }

    private void checkName(String entityName) {
        if (StringUtils.isBlank(entityName)) {
            throw FalconWebException.newAPIException("Instance name is mandatory and shouldn't be blank");
        }
    }

    private LineageGraphResult triage(EntityType entityType, Entity entity, String instanceTime, Cluster cluster)
        throws FalconException {

        Date instanceDate = SchemaHelper.parseDateUTC(instanceTime);
        LineageGraphResult result = new LineageGraphResult();
        Set<String> vertices = new HashSet<>();
        Set<LineageGraphResult.Edge> edges = new HashSet<>();
        Map<String, String> instanceStatusMap = new HashMap<>();

        // queue containing all instances which need to be triaged
        Queue<SchedulableEntityInstance> remainingInstances = new LinkedList<>();
        SchedulableEntityInstance currentInstance = new SchedulableEntityInstance(entity.getName(), cluster.getName(),
                instanceDate, entityType);
        remainingInstances.add(currentInstance);

        while (!remainingInstances.isEmpty()) {
            currentInstance = remainingInstances.remove();
            if (currentInstance.getEntityType() == EntityType.FEED) {
                Feed feed = ConfigurationStore.get().get(EntityType.FEED, currentInstance.getEntityName());
                FeedInstanceStatus.AvailabilityStatus status = getFeedInstanceStatus(feed,
                        currentInstance.getInstanceTime(), cluster);

                // add vertex to the graph
                vertices.add(currentInstance.toString());
                if (status == null) {
                    instanceStatusMap.put(currentInstance.toString(), "[ Not Available ]");
                } else {
                    instanceStatusMap.put(currentInstance.toString(), "[" + status.name() + "]");
                    if (status == FeedInstanceStatus.AvailabilityStatus.AVAILABLE) {
                        continue;
                    }
                }

                // find producer process instance and add it to the queue
                SchedulableEntityInstance producerInstance = FeedHelper.getProducerInstance(feed,
                        currentInstance.getInstanceTime(), cluster);
                if (producerInstance != null) {
                    remainingInstances.add(producerInstance);

                    //add edge from producerProcessInstance to the feedInstance
                    LineageGraphResult.Edge edge = new LineageGraphResult.Edge(producerInstance.toString(),
                            currentInstance.toString(), "produces");
                    edges.add(edge);
                }
            } else { // entity type is PROCESS
                Process process = ConfigurationStore.get().get(EntityType.PROCESS, currentInstance.getEntityName());
                InstancesResult.WorkflowStatus status = getProcessInstanceStatus(process,
                        currentInstance.getInstanceTime());

                // add current process instance as a vertex
                vertices.add(currentInstance.toString());
                if (status == null) {
                    instanceStatusMap.put(currentInstance.toString(), "[ Not Available ]");
                } else {
                    instanceStatusMap.put(currentInstance.toString(), "[" + status.name() + "]");
                    if (status == InstancesResult.WorkflowStatus.SUCCEEDED) {
                        continue;
                    }
                }

                // find list of input feed instances - only mandatory ones and not optional ones
                Set<SchedulableEntityInstance> inputFeedInstances = ProcessHelper.getInputFeedInstances(process,
                        currentInstance.getInstanceTime(), cluster, false);
                for (SchedulableEntityInstance inputFeedInstance : inputFeedInstances) {
                    remainingInstances.add(inputFeedInstance);

                    //Add edge from inputFeedInstance to consumer processInstance
                    LineageGraphResult.Edge edge = new LineageGraphResult.Edge(inputFeedInstance.toString(),
                            currentInstance.toString(), "consumed by");
                    edges.add(edge);
                }
            }
        }

        // append status to each vertex
        Set<String> relabeledVertices = new HashSet<>();
        for (String instance : vertices) {
            String status = instanceStatusMap.get(instance);
            relabeledVertices.add(instance + status);
        }

        // append status to each edge
        for (LineageGraphResult.Edge edge : edges) {
            String oldTo = edge.getTo();
            String oldFrom = edge.getFrom();

            String newFrom = oldFrom + instanceStatusMap.get(oldFrom);
            String newTo = oldTo + instanceStatusMap.get(oldTo);

            edge.setFrom(newFrom);
            edge.setTo(newTo);
        }

        result.setEdges(edges.toArray(new LineageGraphResult.Edge[0]));
        result.setVertices(relabeledVertices.toArray(new String[0]));
        return result;
    }

    private FeedInstanceStatus.AvailabilityStatus getFeedInstanceStatus(Feed feed, Date instanceTime, Cluster cluster)
        throws FalconException {
        Storage storage = FeedHelper.createStorage(cluster, feed);
        Date endRange = new Date(instanceTime.getTime() + 200);
        List<FeedInstanceStatus> feedListing = storage.getListing(feed, cluster.getName(), LocationType.DATA,
                instanceTime, endRange);
        if (feedListing.size() > 0) {
            return feedListing.get(0).getStatus();
        }
        LOG.warn("No instances were found for the given feed: {} & instanceTime: {}", feed, instanceTime);
        return null;
    }

    private InstancesResult.WorkflowStatus getProcessInstanceStatus(Process process, Date instanceTime)
        throws FalconException {
        AbstractWorkflowEngine wfEngine = getWorkflowEngine(process);
        List<LifeCycle> lifeCycles = new ArrayList<LifeCycle>();
        lifeCycles.add(LifeCycle.valueOf(LifeCycle.EXECUTION.name()));
        Date endRange = new Date(instanceTime.getTime() + 200);
        Instance[] response = wfEngine.getStatus(process, instanceTime, endRange, lifeCycles, null).getInstances();
        if (response.length > 0) {
            return response[0].getStatus();
        }
        LOG.warn("No instances were found for the given process: {} & instanceTime: {}", process, instanceTime);
        return null;
    }

    public InstancesResult reRunInstance(String type, String entity, String startStr, String endStr,
                                         HttpServletRequest request, String colo, List<LifeCycle> lifeCycles,
                                         Boolean isForced) {
        Properties props = getProperties(request);
        return reRunInstance(type, entity, startStr, endStr, props, colo, lifeCycles, isForced);
    }

    public InstancesResult reRunInstance(String type, String entity, String startStr, String endStr, Properties props,
                                         String colo, List<LifeCycle> lifeCycles, Boolean isForced) {
        checkColo(colo);
        checkType(type);
        if (StartupProperties.isServerInSafeMode()) {
            throwSafemodeException("RERUN");
        }
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDateForLifecycleOperations(
                    entityObject, startStr, endStr);

            AbstractWorkflowEngine wfEngine = getWorkflowEngine(entityObject);
            return wfEngine.reRunInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles, isForced);
        } catch (Exception e) {
            LOG.error("Failed to rerun instances", e);
            throw FalconWebException.newAPIException(e);
        }
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private Properties getProperties(HttpServletRequest request) {
        Properties props = new Properties();
        try {
            ServletInputStream xmlStream = request == null ? null : request.getInputStream();
            if (xmlStream != null) {
                if (xmlStream.markSupported()) {
                    xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
                }
                props.load(xmlStream);
            }
        } catch (IOException e) {
            LOG.error("Failed to get properties from request", e);
        }
        return props;
    }

    private Pair<Date, Date> getStartAndEndDateForLifecycleOperations(Entity entityObject,
                                                                      String startStr, String endStr)
        throws FalconException {

        if (StringUtils.isEmpty(startStr) || StringUtils.isEmpty(endStr)) {
            throw new FalconException("Start and End dates cannot be empty for Instance POST apis");
        }

        return getStartAndEndDate(entityObject, startStr, endStr);
    }

    private Pair<Date, Date> getStartAndEndDate(Entity entityObject, String startStr, String endStr)
        throws FalconException {
        return getStartAndEndDate(entityObject, startStr, endStr, getDefaultResultsPerPage());
    }

    protected Pair<Date, Date> getStartAndEndDate(Entity entityObject, String startStr, String endStr,
                                                  Integer numResults) throws FalconException {
        Pair<Date, Date> clusterStartEndDates = EntityUtil.getEntityStartEndDates(entityObject);
        Frequency frequency = EntityUtil.getFrequency(entityObject);
        Date endDate = getEndDate(endStr, clusterStartEndDates.second);
        Date startDate = getStartDate(startStr, endDate, clusterStartEndDates.first, frequency, numResults);
        if (startDate.after(endDate)) {
            throw new IllegalArgumentException("Specified End date "
                    + SchemaHelper.getDateFormat().format(endDate)
                    + " is before the entity was scheduled "
                    + SchemaHelper.getDateFormat().format(startDate));
        }
        return new Pair<>(startDate, endDate);
    }

    private Date getEndDate(String endStr, Date clusterEndDate) throws FalconException {
        Date endDate;
        if (StringUtils.isEmpty(endStr)) {
            endDate = new Date();
        } else {
            endDate = EntityUtil.parseDateUTC(endStr);
        }
        if (endDate.after(clusterEndDate)) {
            endDate = clusterEndDate;
        }
        return endDate;
    }

    private Date getStartDate(String startStr, Date end, Date clusterStartDate,
                              Frequency frequency, final int dateMultiplier) throws FalconException {
        Date start;
        if (StringUtils.isEmpty(startStr)) {
            // set startDate to endDate - dateMultiplier times frequency
            long startMillis = end.getTime();

            switch (frequency.getTimeUnit().getCalendarUnit()){
            case Calendar.MINUTE :
                startMillis -= frequency.getFrequencyAsInt() * MINUTE_IN_MILLIS * dateMultiplier;
                break;

            case Calendar.HOUR :
                startMillis -= frequency.getFrequencyAsInt() * HOUR_IN_MILLIS * dateMultiplier;
                break;

            case Calendar.DATE :
                startMillis -= frequency.getFrequencyAsInt() * DAY_IN_MILLIS * dateMultiplier;
                break;

            case Calendar.MONTH :
                startMillis -= frequency.getFrequencyAsInt() * MONTH_IN_MILLIS * dateMultiplier;
                break;

            default:
                break;
            }

            start = new Date(startMillis);
            if (start.after(end)) {
                LOG.warn("Calculated start date : {} crossed end date : {} setting it to "
                        + "entity start date", start, end);
                start = clusterStartDate;
            }
        } else {
            start = EntityUtil.parseDateUTC(startStr);
        }

        if (start.before(clusterStartDate)) {
            start = clusterStartDate;
        }

        return start;
    }

    protected void validateParams(String type, String entity) throws FalconException {
        validateNotEmpty("entityType", type);
        validateNotEmpty("entityName", entity);
    }

    private void validateNotEmpty(String field, String param) throws ValidationException {
        if (StringUtils.isEmpty(param)) {
            throw new ValidationException("Parameter " + field + " is empty");
        }
    }

    private boolean containsIgnoreCase(List<String> strList, String str) {
        for (String s : strList) {
            if (s.equalsIgnoreCase(str)) {
                return true;
            }
        }
        return false;
    }

    private Date getEarliestDate(List<String> dateList) throws FalconException {
        if (dateList.size() == 1) {
            return EntityUtil.parseDateUTC(dateList.get(0));
        }
        Date earliestDate = EntityUtil.parseDateUTC(dateList.get(0));
        for (int i = 1; i < dateList.size(); i++) {
            if (earliestDate.after(EntityUtil.parseDateUTC(dateList.get(i)))) {
                earliestDate = EntityUtil.parseDateUTC(dateList.get(i));
            }
        }
        return earliestDate;
    }

    private void throwSafemodeException(String operation) {
        String error = "Instance operation " + operation + " cannot be performed when server is in safemode";
        LOG.error(error);
        throw FalconWebException.newAPIException(error);
    }
}
