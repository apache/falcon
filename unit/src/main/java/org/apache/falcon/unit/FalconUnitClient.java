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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.client.AbstractFalconClient;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.admin.AdminResource;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Client for Falcon Unit.
 */
public class FalconUnitClient extends AbstractFalconClient {

    private static final Logger LOG = LoggerFactory.getLogger(FalconUnitClient.class);
    protected static final int XML_DEBUG_LEN = 10 * 1024;

    private static final String DEFAULT_ORDERBY = "status";
    private static final String DEFAULT_SORTED_ORDER = "asc";

    protected ConfigurationStore configStore;
    private AbstractWorkflowEngine workflowEngine;
    private LocalSchedulableEntityManager localSchedulableEntityManager;
    private LocalInstanceManager localInstanceManager;


    public FalconUnitClient() throws FalconException {
        configStore = ConfigurationStore.get();
        workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
        localSchedulableEntityManager = new LocalSchedulableEntityManager();
        localInstanceManager = new LocalInstanceManager();
    }

    public ConfigurationStore getConfigStore() {
        return this.configStore;
    }


    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system
     *
     * @param type     entity type
     * @param filePath path for the definition of entity
     * @return boolean
     */
    @Override
    public APIResult submit(String type, String filePath, String doAsUser) throws IOException, FalconCLIException {

        try {
            return localSchedulableEntityManager.submit(type, filePath, doAsUser);
        } catch (FalconException e) {
            throw new FalconCLIException("FAILED", e);
        }
    }

    /**
     * Schedules submitted entity.
     *
     * @param entityType entity Type
     * @param entityName entity name
     * @param cluster    cluster on which it has to be scheduled
     * @return
     * @throws FalconCLIException
     * @throws FalconException
     */
    @Override
    public APIResult schedule(EntityType entityType, String entityName, String cluster,
                              Boolean skipDryRun, String doAsUser, String properties) throws FalconCLIException {
        try {
            return localSchedulableEntityManager.schedule(entityType, entityName, skipDryRun, properties);
        } catch (FalconException e) {
            throw new FalconCLIException(e);
        } catch (AuthorizationException e) {
            throw new FalconCLIException(e);
        }
    }

    @Override
    public APIResult delete(EntityType entityType, String entityName, String doAsUser) {
        return localSchedulableEntityManager.delete(entityType, entityName, doAsUser);
    }

    @Override
    public APIResult validate(String entityType, String filePath, Boolean skipDryRun,
                              String doAsUser) throws FalconCLIException {
        try {
            return localSchedulableEntityManager.validate(entityType, filePath, skipDryRun, doAsUser);
        } catch (FalconException e) {
            throw new FalconCLIException(e);
        }
    }

    @Override
    public APIResult update(String entityType, String entityName, String filePath,
                            Boolean skipDryRun, String doAsUser) throws FalconCLIException {
        try {
            return localSchedulableEntityManager.update(entityType, entityName, filePath,
                    skipDryRun, "local", doAsUser);
        } catch (FalconException e) {
            throw new FalconCLIException(e);
        }
    }

    @Override
    public Entity getDefinition(String entityType, String entityName, String doAsUser) throws FalconCLIException {
        String entity = localSchedulableEntityManager.getEntityDefinition(entityType, entityName);
        return Entity.fromString(EntityType.getEnum(entityType), entity);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @Override
    public InstancesResult getStatusOfInstances(String type, String entity, String start, String end,
                                                String colo, List<LifeCycle> lifeCycles, String filterBy,
                                                String orderBy, String sortOrder, Integer offset,
                                                Integer numResults, String doAsUser) throws FalconCLIException {
        if (orderBy == null) {
            orderBy = DEFAULT_ORDERBY;
        }
        if (sortOrder == null) {
            sortOrder = DEFAULT_SORTED_ORDER;
        }
        if (offset == null) {
            offset = 0;
        }
        if (numResults == null) {
            numResults = 1;
        }
        return localInstanceManager.getStatusOfInstances(type, entity, start, end, colo, lifeCycles, filterBy, orderBy,
                sortOrder, offset, numResults);

    }


    /**
     * Schedules an submitted process entity immediately.
     *
     * @param entityName   entity name
     * @param startTime    start time for process while scheduling
     * @param numInstances numInstances of process to be scheduled
     * @param cluster      cluster on which process to be scheduled
     * @return boolean
     */
    public APIResult schedule(EntityType entityType, String entityName, String startTime, int numInstances,
                              String cluster, Boolean skipDryRun, String properties) throws FalconCLIException {
        try {
            FalconUnitHelper.checkSchedulableEntity(entityType.toString());
            Entity entity = EntityUtil.getEntity(entityType, entityName);
            boolean clusterPresent = checkAndUpdateCluster(entity, entityType, cluster);
            if (!clusterPresent) {
                LOG.warn("Cluster is not registered with this entity " + entityName);
                return new APIResult(APIResult.Status.FAILED, entity + "Cluster is not registered with this entity "
                        + entityName);
            }
            if (StringUtils.isNotEmpty(startTime) && entityType == EntityType.PROCESS) {
                updateStartAndEndTime((Process) entity, startTime, numInstances, cluster);
            }
            workflowEngine.schedule(entity, skipDryRun, EntityUtil.getPropertyMap(properties));
            LOG.info(entityName + " is scheduled successfully");
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + "PROCESS" + ") scheduled successfully");
        } catch (FalconException e) {
            throw new FalconCLIException("FAILED", e);
        }
    }

    /**
     * Instance status for a given nominalTime.
     *
     * @param entityType  entity type
     * @param entityName  entity name
     * @param nominalTime nominal time of process
     * @return InstancesResult.WorkflowStatus
     */
    public InstancesResult.WorkflowStatus getInstanceStatus(String entityType, String entityName,
                                                            String nominalTime) throws Exception {
        Date startTime = SchemaHelper.parseDateUTC(nominalTime);
        Date endTimeDate = DateUtil.getNextMinute(startTime);
        String endTime = DateUtil.getDateFormatFromTime(endTimeDate.getTime());
        InstancesResult instancesResult = getStatusOfInstances(entityType, entityName, nominalTime, endTime, null,
                null, null, null, null, null, null, null);
        if (instancesResult.getInstances() != null && instancesResult.getInstances().length > 0
                && instancesResult.getInstances()[0] != null) {
            LOG.info("Instance status is " + instancesResult.getInstances()[0].getStatus());
            return instancesResult.getInstances()[0].getStatus();
        }
        return null;
    }

    @Override
    public APIResult suspend(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException {
        return localSchedulableEntityManager.suspend(entityType.name(), entityName, colo);
    }

    @Override
    public APIResult resume(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException {
        return localSchedulableEntityManager.resume(entityType.name(), entityName, colo);
    }

    @Override
    public APIResult getStatus(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException {
        return localSchedulableEntityManager.getStatus(entityType.name(), entityName, colo);
    }

    @Override
    public APIResult submitAndSchedule(String entityType, String filePath, Boolean skipDryRun, String doAsUser,
                                       String properties) throws FalconCLIException {
        try {
            return localSchedulableEntityManager.submitAndSchedule(entityType, filePath, skipDryRun, doAsUser,
                    properties);
        } catch (FalconException e) {
            throw new FalconCLIException(e);
        } catch (IOException e) {
            throw new FalconCLIException(e);
        }
    }

    @Override
    public EntityList getEntityList(String entityType, String fields, String nameSubsequence, String tagKeywords,
                                    String filterBy, String filterTags, String orderBy, String sortOrder,
                                    Integer offset, Integer numResults, String doAsUser) throws FalconCLIException {
        return localSchedulableEntityManager.getEntityList(fields, nameSubsequence, tagKeywords, entityType, filterTags,
                filterBy, orderBy, sortOrder, offset, numResults, doAsUser);
    }

    @Override
    public EntitySummaryResult getEntitySummary(String entityType, String cluster, String start, String end,
                                                String fields, String filterBy, String filterTags, String orderBy,
                                                String sortOrder, Integer offset, Integer numResults,
                                                Integer numInstances, String doAsUser) throws FalconCLIException {
        return localSchedulableEntityManager.getEntitySummary(entityType, cluster, start, end, fields, filterBy,
                filterTags, orderBy, sortOrder, offset, numResults, numInstances, doAsUser);
    }

    @Override
    public APIResult touch(String entityType, String entityName, String colo, Boolean skipDryRun,
                           String doAsUser) throws FalconCLIException {
        return localSchedulableEntityManager.touch(entityType, entityName, colo, skipDryRun);
    }

    public InstancesResult killInstances(String type, String entity, String start, String end, String colo,
                                         String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                         String doAsUser) throws FalconCLIException, UnsupportedEncodingException {
        Properties props = getProperties(clusters, sourceClusters);
        return localInstanceManager.killInstance(props, type, entity, start, end, colo, lifeCycles);
    }

    public InstancesResult suspendInstances(String type, String entity, String start, String end, String colo,
                                            String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                            String doAsUser) throws FalconCLIException, UnsupportedEncodingException {
        Properties props = getProperties(clusters, sourceClusters);
        return localInstanceManager.suspendInstance(props, type, entity, start, end, colo, lifeCycles);
    }

    public InstancesResult resumeInstances(String type, String entity, String start, String end, String colo,
                                           String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                           String doAsUser) throws FalconCLIException, UnsupportedEncodingException {
        Properties props = getProperties(clusters, sourceClusters);
        return localInstanceManager.resumeInstance(props, type, entity, start, end, colo, lifeCycles);
    }

    public InstancesResult rerunInstances(String type, String entity, String start, String end, String filePath,
                                          String colo, String clusters, String sourceClusters,
                                          List<LifeCycle> lifeCycles, Boolean isForced, String doAsUser) throws
            FalconCLIException, IOException {
        Properties props = getProperties(clusters, sourceClusters);
        return localInstanceManager.reRunInstance(type, entity, start, end, props, colo, lifeCycles, isForced);
    }

    public InstancesSummaryResult getSummaryOfInstances(String type, String entity, String start, String end,
                                                        String colo, List<LifeCycle> lifeCycles, String filterBy,
                                                        String orderBy, String sortOrder, String doAsUser) throws
            FalconCLIException {
        return localInstanceManager.getSummary(type, entity, start, end, colo, lifeCycles, filterBy, orderBy,
                sortOrder);
    }

    public FeedInstanceResult getFeedListing(String type, String entity, String start, String end, String colo,
                                             String doAsUser) throws FalconCLIException {
        return localInstanceManager.getListing(type, entity, start, end, colo);
    }

    public InstancesResult getLogsOfInstances(String type, String entity, String start, String end, String colo,
                                              String runId, List<LifeCycle> lifeCycles, String filterBy,
                                              String orderBy, String sortOrder, Integer offset, Integer numResults,
                                              String doAsUser) throws FalconCLIException {
        return localInstanceManager.getLogs(type, entity, start, end, colo, runId, lifeCycles, filterBy, orderBy,
                sortOrder, offset, numResults);
    }

    public InstancesResult getParamsOfInstance(String type, String entity, String start, String colo,
                                               List<LifeCycle> lifeCycles, String doAsUser) throws FalconCLIException,
            UnsupportedEncodingException {
        return localInstanceManager.getInstanceParams(type, entity, start, colo, lifeCycles);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    public InstanceDependencyResult getInstanceDependencies(String entityType, String entityName, String instanceTime,
                                                            String colo) throws FalconCLIException {
        return localInstanceManager.getInstanceDependencies(entityType, entityName, instanceTime, colo);
    }

    @Override
    public String getVersion(String doAsUser) throws FalconCLIException {
        AdminResource resource = new AdminResource();
        AdminResource.PropertyList propertyList = resource.getVersion();
        StringBuilder properties = new StringBuilder();
        for(AdminResource.Property property : propertyList.properties) {
            if (properties.length() > 1) {
                properties.append(",");
            }
            properties.append(property.key).append(":").append(property.value);
        }
        return properties.toString();
    }

    private boolean checkAndUpdateCluster(Entity entity, EntityType entityType, String cluster) {
        if (entityType == EntityType.FEED) {
            return checkAndUpdateFeedClusters(entity, cluster);
        } else if (entityType == EntityType.PROCESS) {
            return checkAndUpdateProcessClusters(entity, cluster);
        } else {
            throw new IllegalArgumentException("entity type {} is not supported " + entityType);
        }
    }

    private boolean checkAndUpdateProcessClusters(Entity entity, String cluster) {
        Process processEntity = (Process) entity;
        List<Cluster> clusters = processEntity.getClusters().getClusters();
        List<Cluster> newClusters = new ArrayList<>();
        if (clusters != null) {
            for (Cluster processCluster : clusters) {
                if (processCluster.getName().equalsIgnoreCase(cluster)) {
                    newClusters.add(processCluster);
                }
            }
        }
        if (newClusters.isEmpty()) {
            LOG.warn("Cluster is not registered with this entity " + entity.getName());
            return false;
        }
        processEntity.getClusters().getClusters().removeAll(clusters);
        processEntity.getClusters().getClusters().addAll(newClusters);
        return true;
    }

    private boolean checkAndUpdateFeedClusters(Entity entity, String cluster) {
        Feed feedEntity = (Feed) entity;
        List<org.apache.falcon.entity.v0.feed.Cluster> clusters = feedEntity.getClusters().getClusters();
        List<org.apache.falcon.entity.v0.feed.Cluster> newClusters = new ArrayList<>();
        if (clusters != null) {
            for (org.apache.falcon.entity.v0.feed.Cluster feedClusters : clusters) {
                if (feedClusters.getName().equalsIgnoreCase(cluster)) {
                    newClusters.add(feedClusters);
                }
            }
        }
        if (newClusters.isEmpty()) {
            LOG.warn("Cluster is not registered with this entity " + entity.getName());
            return false;
        }
        feedEntity.getClusters().getClusters().removeAll(clusters);
        feedEntity.getClusters().getClusters().addAll(newClusters);
        return true;
    }

    private void updateStartAndEndTime(Process processEntity, String startTimeStr, int numInstances, String cluster) {
        List<Cluster> clusters = processEntity.getClusters().getClusters();
        if (clusters != null) {
            for (Cluster processCluster : clusters) {
                if (processCluster.getName().equalsIgnoreCase(cluster)) {
                    Validity validity = new Validity();
                    Date startTime = SchemaHelper.parseDateUTC(startTimeStr);
                    validity.setStart(startTime);
                    Date endTime = EntityUtil.getNextInstanceTime(startTime, processEntity.getFrequency(),
                            TimeZone.getTimeZone("UTC"), numInstances);
                    validity.setEnd(endTime);
                    processCluster.setValidity(validity);
                }
            }
        }
    }

    private Properties getProperties(String clusters, String sourceClusters) {
        Properties props = new Properties();
        if (StringUtils.isNotEmpty(clusters)) {
            props.setProperty(FALCON_INSTANCE_ACTION_CLUSTERS, clusters);
        }
        if (StringUtils.isNotEmpty(sourceClusters)) {
            props.setProperty(FALCON_INSTANCE_SOURCE_CLUSTERS, sourceClusters);
        }
        return props;
    }
}
