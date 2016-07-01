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
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.lock.MemoryLocks;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.UnschedulableEntityException;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.service.EntitySLAMonitoringService;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * REST resource of allowed actions on Schedulable Entities, Only Process and
 * Feed can have schedulable actions.
 */
public abstract class AbstractSchedulableEntityManager extends AbstractInstanceManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSchedulableEntityManager.class);
    private static MemoryLocks memoryLocks = MemoryLocks.getInstance();

    /**
     * Schedules a submitted entity immediately.
     *
     * @param type   entity type
     * @param entity entity name
     * @param properties Specifying 'falcon.scheduler:native' as a property will schedule the entity on the
     *                   native workflow engine, else it will default to the workflow engine
     *                   as defined in startup.properties.
     * @return APIResult
     */
    public APIResult schedule(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("entityName") @PathParam("entity") String entity,
            @Dimension("colo") @PathParam("colo") String colo,
            @QueryParam("skipDryRun") Boolean skipDryRun,
            @QueryParam("properties") String properties) {
        checkColo(colo);
        try {
            scheduleInternal(type, entity, skipDryRun,  EntityUtil.getPropertyMap(properties));
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + type + ") scheduled successfully");
        } catch (Throwable e) {
            LOG.error("Unable to schedule workflow", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    protected synchronized void scheduleInternal(String type, String entity, Boolean skipDryRun,
                                 Map<String, String> properties) throws FalconException, AuthorizationException {

        checkSchedulableEntity(type);
        Entity entityObj = null;
        try {
            entityObj = EntityUtil.getEntity(type, entity);
            verifySafemodeOperation(entityObj, EntityUtil.ENTITY_OPERATION.SCHEDULE);
            //first acquire lock on entity before scheduling
            if (!memoryLocks.acquireLock(entityObj, "schedule")) {
                throw  FalconWebException.newAPIException("Looks like an schedule/update command is already"
                        + " running for " + entityObj.toShortString());
            }
            LOG.info("Memory lock obtained for {} by {}", entityObj.toShortString(), Thread.currentThread().getName());
            WorkflowEngineFactory.getWorkflowEngine(entityObj, properties).schedule(entityObj, skipDryRun, properties);
        } catch (Throwable e) {
            LOG.error("Entity schedule failed for " + type + ": " + entity, e);
            throw FalconWebException.newAPIException(e);
        } finally {
            if (entityObj != null) {
                memoryLocks.releaseLock(entityObj);
                LOG.info("Memory lock released for {}", entityObj.toShortString());
            }
        }
    }

    public static void validateSlaParams(String entityType, String entityName, String start, String end,
                                         String colo) throws FalconException {
        EntityType type = EntityType.getEnum(entityType);
        if (type != EntityType.FEED) {
            throw new ValidationException("SLA monitoring is not supported for: " + type);
        }

        // validate valid feed name.
        if (StringUtils.isNotBlank(entityName)) {
            EntityUtil.getEntity(EntityType.FEED, entityName);
        }

        Date startTime, endTime;
        // validate mandatory start date
        if (StringUtils.isBlank(start)) {
            throw new ValidationException("'start' is mandatory and can not be blank.");
        } else {
            startTime = SchemaHelper.parseDateUTC(start);
        }

        // validate optional end date
        if (StringUtils.isBlank(end)) {
            endTime = new Date();
        } else {
            endTime = SchemaHelper.parseDateUTC(end);
        }

        if (startTime.after(endTime)) {
            throw new ValidationException("start can not be after end");
        }

        checkColo(colo);
    }

    /**
     * Returns the feed instances which are not yet available and have missed either slaLow or slaHigh.
     * This api doesn't return the feeds which missed SLA but are now available. Purpose of this api is to show feed
     * instances which you need to attend to.
     * @param startStr startTime in
     * @param endStr
     */
    public SchedulableEntityInstanceResult getFeedSLAMissPendingAlerts(String feedName, String startStr, String endStr,
                                                                       String colo) {

        Set<SchedulableEntityInstance> instances = new HashSet<>();
        try {
            checkColo(colo);
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = (endStr == null) ? new Date() : EntityUtil.parseDateUTC(endStr);

            if (StringUtils.isBlank(feedName)) {
                instances.addAll(EntitySLAMonitoringService.get().getEntitySLAMissPendingAlerts(start, end));
            } else {
                for (String clusterName : DeploymentUtil.getCurrentClusters()) {
                    instances.addAll(EntitySLAMonitoringService.get().getEntitySLAMissPendingAlerts(feedName,
                            clusterName, start, end, EntityType.FEED.toString()));
                }
            }
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e);
        }
        SchedulableEntityInstanceResult result = new SchedulableEntityInstanceResult(APIResult.Status.SUCCEEDED,
                "Success!");
        result.setCollection(instances.toArray());
        return result;
    }

    /**
     * Submits a new entity and schedules it immediately.
     *
     * @param type   entity type
     * @param properties Specifying 'falcon.scheduler:native' as a property will schedule the entity on the
     *                   native workflow engine, else it will default to the workflow engine
     *                   as defined in startup.properties.
     * @return APIResult
     */
    public APIResult submitAndSchedule(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("colo") @PathParam("colo") String colo,
            @QueryParam("skipDryRun") Boolean skipDryRun,
            @QueryParam("properties") String properties) {
        checkColo(colo);
        try {
            checkSchedulableEntity(type);
            Entity entity = submitInternal(request.getInputStream(), type, request.getParameter(DO_AS_PARAM));
            scheduleInternal(type, entity.getName(), skipDryRun, EntityUtil.getPropertyMap(properties));
            return new APIResult(APIResult.Status.SUCCEEDED,
                    entity.getName() + "(" + type + ") scheduled successfully");
        } catch (Throwable e) {
            LOG.error("Unable to submit and schedule ", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    /**
     * Suspends a running entity.
     *
     * @param type   entity type
     * @param entity entity name
     * @return APIResult
     */
    public APIResult suspend(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("entityName") @PathParam("entity") String entity,
            @Dimension("entityName") @PathParam("entity") String colo) {
        checkColo(colo);
        try {
            checkSchedulableEntity(type);
            Entity entityObj = EntityUtil.getEntity(type, entity);
            verifySafemodeOperation(entityObj, EntityUtil.ENTITY_OPERATION.SUSPEND);
            if (getWorkflowEngine(entityObj).isActive(entityObj)) {
                getWorkflowEngine(entityObj).suspend(entityObj);
            } else {
                throw  FalconWebException.newAPIException(entity + "(" + type + ") is not scheduled");
            }
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + type + ") suspended successfully");
        } catch (Throwable e) {
            LOG.error("Unable to suspend entity", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    /**
     * Resumes a suspended entity.
     *
     * @param type   entity type
     * @param entity entity name
     * @return APIResult
     */
    public APIResult resume(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("entityName") @PathParam("entity") String entity,
            @Dimension("colo") @PathParam("colo") String colo) {

        checkColo(colo);
        try {
            checkSchedulableEntity(type);
            Entity entityObj = EntityUtil.getEntity(type, entity);
            verifySafemodeOperation(entityObj, EntityUtil.ENTITY_OPERATION.RESUME);
            if (getWorkflowEngine(entityObj).isActive(entityObj)) {
                getWorkflowEngine(entityObj).resume(entityObj);
            } else {
                throw new IllegalStateException(entity + "(" + type + ") is not scheduled");
            }
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + type + ") resumed successfully");
        } catch (Exception e) {
            LOG.error("Unable to resume entity", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    /**
     * Returns summary of most recent N instances of an entity, filtered by cluster.
     *
     * @param type           Only return entities of this type.
     * @param startDate      For each entity, show instances after startDate.
     * @param endDate        For each entity, show instances before endDate.
     * @param cluster        Return entities for specific cluster.
     * @param fields       fields that the query is interested in, separated by comma
     * @param filterBy       filter by a specific field.
     * @param filterTags     filter by these tags.
     * @param orderBy        order result by these fields.
     * @param offset         Pagination offset.
     * @param resultsPerPage Number of results that should be returned starting at the offset.
     * @param numInstances   Number of instance summaries to show per entity
     * @return EntitySummaryResult
     */
    public EntitySummaryResult getEntitySummary(String type, String cluster, String startDate, String endDate,
                                                String fields, String filterBy, String filterTags,
                                                String orderBy, String sortOrder, Integer offset,
                                                Integer resultsPerPage, Integer numInstances, final String doAsUser) {
        HashSet<String> fieldSet = new HashSet<String>(Arrays.asList(fields.toLowerCase().split(",")));
        Pair<Date, Date> startAndEndDates = getStartEndDatesForSummary(startDate, endDate);
        validateTypeForEntitySummary(type);
        Map<String, List<String>> filterByFieldsValues = getFilterByFieldsValues(filterBy);
        validateEntityFilterByClause(filterByFieldsValues);
        if (StringUtils.isNotEmpty(filterTags)) {
            filterByFieldsValues.put(EntityList.EntityFilterByFields.TAGS.name(), Arrays.asList(filterTags));
        }

        List<Entity> entities;
        String colo;
        try {
            entities = sortEntitiesPagination(
                    getFilteredEntities(EntityType.valueOf(type.toUpperCase()), "", "", filterByFieldsValues,
                            SchemaHelper.getDateFormat().format(startAndEndDates.first),
                            SchemaHelper.getDateFormat().format(startAndEndDates.second),
                            cluster, doAsUser),
                    orderBy, sortOrder, offset, resultsPerPage);
            colo = ((Cluster) configStore.get(EntityType.CLUSTER, cluster)).getColo();
        } catch (FalconWebException e) {
            throw e;
        } catch(Exception e) {
            LOG.error("Failed to get entities", e);
            throw FalconWebException.newAPIException(e);
        }

        List<EntitySummaryResult.EntitySummary> entitySummaries = new ArrayList<EntitySummaryResult.EntitySummary>();
        for (Entity entity : entities) {
            InstancesResult instancesResult = getInstances(entity.getEntityType().name(), entity.getName(),
                    SchemaHelper.getDateFormat().format(startAndEndDates.first),
                    SchemaHelper.getDateFormat().format(startAndEndDates.second),
                    colo, null, "", "", "", 0, numInstances, null);

            /* ToDo - Use oozie bulk API after FALCON-591 is implemented
             *       getBulkInstances(entity, cluster,
             *      startAndEndDates.first, startAndEndDates.second, colo, "starttime", 0, numInstances);
             */
            List<EntitySummaryResult.Instance> entitySummaryInstances =
                    getElementsFromInstanceResult(instancesResult);

            List<String> pipelines = new ArrayList<String>();
            List<String> tags = new ArrayList<String>();
            if (fieldSet.contains("pipelines")) { pipelines = EntityUtil.getPipelines(entity); }
            if (fieldSet.contains("tags")) { tags = EntityUtil.getTags(entity); }

            EntitySummaryResult.EntitySummary entitySummary =
                    new EntitySummaryResult.EntitySummary(entity.getName(), entity.getEntityType().toString(),
                            getStatusString(entity),
                            tags.toArray(new String[tags.size()]),
                            pipelines.toArray(new String[pipelines.size()]),
                            entitySummaryInstances.toArray(
                                    new EntitySummaryResult.Instance[entitySummaryInstances.size()]));
            entitySummaries.add(entitySummary);
        }
        return new EntitySummaryResult("Entity Summary Result",
                entitySummaries.toArray(new EntitySummaryResult.EntitySummary[entitySummaries.size()]));
    }

    /**
     * Force updates an entity.
     *
     * @param type
     * @param entityName
     * @return APIResult
     */
    public APIResult touch(@Dimension("entityType") @PathParam("type") String type,
                           @Dimension("entityName") @PathParam("entity") String entityName,
                           @Dimension("colo") @QueryParam("colo") String colo,
                           @QueryParam("skipDryRun") Boolean skipDryRun) {
        checkColo(colo);
        StringBuilder result = new StringBuilder();
        try {
            Entity entity = EntityUtil.getEntity(type, entityName);
            verifySafemodeOperation(entity, EntityUtil.ENTITY_OPERATION.TOUCH);
            decorateEntityWithACL(entity);
            Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
            for (String cluster : clusters) {
                result.append(getWorkflowEngine(entity).touch(entity, cluster, skipDryRun));
            }
        } catch (Throwable e) {
            LOG.error("Touch failed", e);
            throw FalconWebException.newAPIException(e);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, result.toString());
    }


    private void validateTypeForEntitySummary(String type) {
        EntityType entityType = EntityType.getEnum(type);
        if (!entityType.isSchedulable()) {
            throw FalconWebException.newAPIException("Invalid entity type " + type
                            + " for EntitySummary API. Valid options are feed or process");
        }
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private Pair<Date, Date> getStartEndDatesForSummary(String startDate, String endDate) {
        Date end = (StringUtils.isEmpty(endDate)) ? new Date() : SchemaHelper.parseDateUTC(endDate);

        long startMillisecs = end.getTime() - (2* DAY_IN_MILLIS); // default - 2 days before end
        Date start = (StringUtils.isEmpty(startDate))
                ? new Date(startMillisecs) : SchemaHelper.parseDateUTC(startDate);

        return new Pair<Date, Date>(start, end);
    }

    private List<EntitySummaryResult.Instance> getElementsFromInstanceResult(InstancesResult instancesResult) {
        ArrayList<EntitySummaryResult.Instance> elemInstanceList =
                new ArrayList<EntitySummaryResult.Instance>();
        InstancesResult.Instance[] instances = instancesResult.getInstances();
        if (instances != null && instances.length > 0) {
            for (InstancesResult.Instance rawInstance : instances) {
                EntitySummaryResult.Instance instance = new EntitySummaryResult.Instance(rawInstance.getCluster(),
                        rawInstance.getInstance(),
                        EntitySummaryResult.WorkflowStatus.valueOf(rawInstance.getStatus().toString()));
                instance.logFile = rawInstance.getLogFile();
                instance.sourceCluster = rawInstance.sourceCluster;
                instance.startTime = rawInstance.startTime;
                instance.endTime = rawInstance.endTime;
                elemInstanceList.add(instance);
            }
        }

        return elemInstanceList;
    }

    private void checkSchedulableEntity(String type) throws UnschedulableEntityException {
        EntityType entityType = EntityType.getEnum(type);
        if (!entityType.isSchedulable()) {
            throw new UnschedulableEntityException(
                    "Entity type (" + type + ") " + " cannot be Scheduled/Suspended/Resumed");
        }
    }
}
