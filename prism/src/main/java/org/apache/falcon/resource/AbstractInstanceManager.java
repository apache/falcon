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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.*;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.logging.LogProvider;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

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
            throw FalconWebException.newInstanceException("entity type is empty",
                    Response.Status.BAD_REQUEST);
        } else {
            EntityType entityType = EntityType.getEnum(type);
            if (entityType == EntityType.CLUSTER) {
                throw FalconWebException.newInstanceException(
                        "Instance management functions don't apply to Cluster entities",
                        Response.Status.BAD_REQUEST);
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
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            Entity entityObject = EntityUtil.getEntity(type, entity);
            return getInstanceResultSubset(wfEngine.getRunningInstances(entityObject, lifeCycles),
                    filterBy, orderBy, sortOrder, offset, numResults);
        } catch (Throwable e) {
            LOG.error("Failed to get running instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    protected void validateInstanceFilterByClause(String entityFilterByClause) {
        Map<String, String> filterByFieldsValues = getFilterByFieldsValues(entityFilterByClause);
        for (Map.Entry<String, String> entry : filterByFieldsValues.entrySet()) {
            try {
                InstancesResult.InstanceFilterFields filterKey =
                        InstancesResult.InstanceFilterFields .valueOf(entry.getKey().toUpperCase());
                if (filterKey == InstancesResult.InstanceFilterFields.STARTEDAFTER) {
                    EntityUtil.parseDateUTC(entry.getValue());
                }
            } catch (IllegalArgumentException e) {
                throw FalconWebException.newInstanceException(
                        "Invalid filter key: " + entry.getKey(), Response.Status.BAD_REQUEST);
            } catch (FalconException e) {
                throw FalconWebException.newInstanceException(
                        "Invalid date value for key: " + entry.getKey(), Response.Status.BAD_REQUEST);
            }
        }
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public InstancesResult getInstances(String type, String entity, String startStr, String endStr,
                                        String colo, List<LifeCycle> lifeCycles,
                                        String filterBy, String orderBy, String sortOrder,
                                        Integer offset, Integer numResults) {
        return getStatus(type, entity, startStr, endStr, colo, lifeCycles,
                filterBy, orderBy, sortOrder, offset, numResults);
    }

    public InstancesResult getStatus(String type, String entity, String startStr, String endStr,
                                     String colo, List<LifeCycle> lifeCycles,
                                     String filterBy, String orderBy, String sortOrder,
                                     Integer offset, Integer numResults) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            validateInstanceFilterByClause(filterBy);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDate(entityObject, startStr, endStr);

            // LifeCycle lifeCycleObject = EntityUtil.getLifeCycle(lifeCycle);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return getInstanceResultSubset(wfEngine.getStatus(entityObject,
                            startAndEndDate.first, startAndEndDate.second, lifeCycles),
                    filterBy, orderBy, sortOrder, offset, numResults);
        } catch (Throwable e) {
            LOG.error("Failed to get instances status", e);
            throw FalconWebException
                    .newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesSummaryResult getSummary(String type, String entity, String startStr, String endStr,
                                             String colo, List<LifeCycle> lifeCycles) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDate(entityObject, startStr, endStr);

            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.getSummary(entityObject, startAndEndDate.first, startAndEndDate.second,
                    lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to get instances status", e);
            throw FalconWebException.newInstanceSummaryException(e, Response.Status.BAD_REQUEST);
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
                    colo, lifeCycles, filterBy, orderBy, sortOrder, offset, numResults);
            LogProvider logProvider = new LogProvider();
            Entity entityObject = EntityUtil.getEntity(type, entity);
            for (Instance instance : result.getInstances()) {
                logProvider.populateLogUrls(entityObject, instance, runId);
            }
            return result;
        } catch (Exception e) {
            LOG.error("Failed to get logs for instances", e);
            throw FalconWebException.newInstanceException(e,
                    Response.Status.BAD_REQUEST);
        }
    }

    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private InstancesResult getInstanceResultSubset(InstancesResult resultSet, String filterBy,
                                                    String orderBy, String sortOrder, Integer offset,
                                                    Integer numResults) throws FalconException {
        if (resultSet.getInstances() == null) {
            // return the empty resultSet
            resultSet.setInstances(new Instance[0]);
            return resultSet;
        }

        // Filter instances
        Map<String, String> filterByFieldsValues = getFilterByFieldsValues(filterBy);
        List<Instance> instanceSet = getFilteredInstanceSet(resultSet, filterByFieldsValues);

        int pageCount = super.getRequiredNumberOfResults(instanceSet.size(), offset, numResults);
        InstancesResult result = new InstancesResult(resultSet.getStatus(), resultSet.getMessage());
        if (pageCount == 0) {
            // return empty result set
            result.setInstances(new Instance[0]);
            return result;
        }
        // Sort the ArrayList using orderBy
        instanceSet = sortInstances(instanceSet, orderBy.toLowerCase(), sortOrder);
        result.setCollection(instanceSet.subList(
                offset, (offset+pageCount)).toArray(new Instance[pageCount]));
        return result;
    }

    private List<Instance> getFilteredInstanceSet(InstancesResult resultSet,
                                                  Map<String, String> filterByFieldsValues)
        throws FalconException {
        // If filterBy is empty, return all instances. Else return instances with matching filter.
        if (filterByFieldsValues.size() == 0) {
            return Arrays.asList(resultSet.getInstances());
        }

        List<Instance> instanceSet = new ArrayList<Instance>();
        for (Instance instance : resultSet.getInstances()) {   // for each instance
            boolean isInstanceFiltered = false;

            // for each filter
            for (Map.Entry<String, String> pair : filterByFieldsValues.entrySet()) {
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

    private boolean isInstanceFiltered(Instance instance,
                                       Map.Entry<String, String> pair) throws FalconException {
        final String filterValue = pair.getValue();
        switch (InstancesResult.InstanceFilterFields.valueOf(pair.getKey().toUpperCase())) {
        case STATUS:
            return instance.getStatus() == null
                    || !instance.getStatus().toString().equalsIgnoreCase(filterValue);

        case CLUSTER:
            return instance.getCluster() == null
                    || !instance.getCluster().equalsIgnoreCase(filterValue);

        case SOURCECLUSTER:
            return instance.getSourceCluster() == null
                    || !instance.getSourceCluster().equalsIgnoreCase(filterValue);

        case STARTEDAFTER:
            return instance.getStartTime() == null
                    || instance.getStartTime().before(EntityUtil.parseDateUTC(filterValue));

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
        }//Default : no sort

        return instanceSet;
    }

    public FeedInstanceResult getListing(String type, String entity, String startStr,
                                         String endStr, String colo) {
        checkColo(colo);
        EntityType entityType = checkType(type);
        try {
            if (entityType != EntityType.FEED) {
                throw new IllegalArgumentException("getLocation is not applicable for " + type);
            }
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDate(entityObject, startStr, endStr);

            return FeedHelper.getFeedInstanceListing(entityObject, startAndEndDate.first, startAndEndDate.second);
        } catch (Throwable e) {
            LOG.error("Failed to get instances listing", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
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
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.getInstanceParams(entityObject, start, end, lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to display params of an instance", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesResult killInstance(HttpServletRequest request,
                                        String type, String entity, String startStr,
                                        String endStr, String colo,
                                        List<LifeCycle> lifeCycles) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDateForLifecycleOperations(
                    entityObject, startStr, endStr);

            Properties props = getProperties(request);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.killInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to kill instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesResult suspendInstance(HttpServletRequest request,
                                           String type, String entity, String startStr,
                                           String endStr, String colo,
                                           List<LifeCycle> lifeCycles) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDateForLifecycleOperations(
                    entityObject, startStr, endStr);

            Properties props = getProperties(request);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.suspendInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to suspend instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesResult resumeInstance(HttpServletRequest request,
                                          String type, String entity, String startStr,
                                          String endStr, String colo,
                                          List<LifeCycle> lifeCycles) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDateForLifecycleOperations(
                    entityObject, startStr, endStr);

            Properties props = getProperties(request);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.resumeInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles);
        } catch (Throwable e) {
            LOG.error("Failed to resume instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public InstancesResult reRunInstance(String type, String entity, String startStr,
                                         String endStr, HttpServletRequest request,
                                         String colo, List<LifeCycle> lifeCycles, Boolean isForced) {
        checkColo(colo);
        checkType(type);
        try {
            lifeCycles = checkAndUpdateLifeCycle(lifeCycles, type);
            validateParams(type, entity);
            Entity entityObject = EntityUtil.getEntity(type, entity);
            Pair<Date, Date> startAndEndDate = getStartAndEndDateForLifecycleOperations(
                    entityObject, startStr, endStr);

            Properties props = getProperties(request);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.reRunInstances(entityObject,
                    startAndEndDate.first, startAndEndDate.second, props, lifeCycles, isForced);
        } catch (Exception e) {
            LOG.error("Failed to rerun instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private Properties getProperties(HttpServletRequest request) throws IOException {
        Properties props = new Properties();
        ServletInputStream xmlStream = request == null ? null : request.getInputStream();
        if (xmlStream != null) {
            if (xmlStream.markSupported()) {
                xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
            }
            props.load(xmlStream);
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
        Pair<Date, Date> clusterStartEndDates = EntityUtil.getEntityStartEndDates(entityObject);
        Frequency frequency = EntityUtil.getFrequency(entityObject);
        Date endDate = getEndDate(startStr, endStr, clusterStartEndDates.second, frequency);
        Date startDate = getStartDate(startStr, endDate, clusterStartEndDates.first, frequency);

        if (startDate.after(endDate)) {
            throw new FalconException("Specified End date " + SchemaHelper.getDateFormat().format(endDate)
                    + " is before the entity was scheduled " + SchemaHelper.getDateFormat().format(startDate));
        }
        return new Pair<Date, Date>(startDate, endDate);
    }

    private Date getEndDate(String startStr, String endStr, Date clusterEndDate,
                            Frequency frequency) throws FalconException {
        Date endDate;
        if (StringUtils.isEmpty(endStr)) {
            if (!StringUtils.isEmpty(startStr)) {
                // set endDate to startDate + 10 times frequency
                endDate = EntityUtil.getNextInstanceTime(EntityUtil.parseDateUTC(startStr), frequency, null, 10);
            } else {
                // set endDate to currentTime
                endDate = new Date();
            }
        } else {
            endDate = EntityUtil.parseDateUTC(endStr);
        }
        if (endDate.after(clusterEndDate)) {
            endDate = clusterEndDate;
        }
        return endDate;
    }

    private Date getStartDate(String startStr, Date end,
                              Date clusterStartDate, Frequency frequency) throws FalconException {
        Date start;
        final int dateMultiplier = 10;
        if (StringUtils.isEmpty(startStr)) {
            // set startDate to endDate - 10 times frequency
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
        } else {
            start = EntityUtil.parseDateUTC(startStr);
        }

        if (start.before(clusterStartDate)) {
            start = clusterStartDate;
        }

        return start;
    }

    private void validateParams(String type, String entity) throws FalconException {
        validateNotEmpty("entityType", type);
        validateNotEmpty("entityName", entity);
    }

    private void validateNotEmpty(String field, String param) throws ValidationException {
        if (StringUtils.isEmpty(param)) {
            throw new ValidationException("Parameter " + field + " is empty");
        }
    }
}
