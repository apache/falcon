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
import org.apache.falcon.ExtensionHandler;
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
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.ExtensionJobList;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.LineageGraphResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.apache.falcon.resource.TriageResult;
import org.apache.falcon.resource.admin.AdminResource;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Properties;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.TimeZone;
import java.util.SortedMap;

/**
 * Client for Falcon Unit.
 */
public class FalconUnitClient extends AbstractFalconClient {

    private static final Logger LOG = LoggerFactory.getLogger(FalconUnitClient.class);
    protected static final int XML_DEBUG_LEN = 10 * 1024;

    private static final String DEFAULT_ORDER_BY = "status";
    private static final String DEFAULT_SORTED_ORDER = "asc";

    private ConfigurationStore configStore;
    private AbstractWorkflowEngine workflowEngine;
    private LocalSchedulableEntityManager localSchedulableEntityManager;
    private LocalInstanceManager localInstanceManager;
    private LocalExtensionManager localExtensionManager;


    FalconUnitClient() throws FalconException {
        configStore = ConfigurationStore.get();
        workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
        localSchedulableEntityManager = new LocalSchedulableEntityManager();
        localInstanceManager = new LocalInstanceManager();
        localExtensionManager = new LocalExtensionManager();
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
    public APIResult submit(String type, String filePath, String doAsUser) {

        try {
            return localSchedulableEntityManager.submit(type, filePath, doAsUser);
        } catch (FalconException | IOException e) {
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
     */
    @Override
    public APIResult schedule(EntityType entityType, String entityName, String cluster,
                              Boolean skipDryRun, String doAsUser, String properties) {
        try {
            return localSchedulableEntityManager.schedule(entityType, entityName, skipDryRun, properties);
        } catch (FalconException | AuthorizationException e) {
            throw new FalconCLIException(e);
        }
    }

    @Override
    public APIResult delete(EntityType entityType, String entityName, String colo) {
        return localSchedulableEntityManager.delete(entityType, entityName, colo);
    }

    @Override
    public APIResult validate(String entityType, String filePath, Boolean skipDryRun,
                              String doAsUser) {
        try {
            return localSchedulableEntityManager.validate(entityType, filePath, skipDryRun, doAsUser);
        } catch (FalconException e) {
            throw new FalconCLIException(e);
        }
    }

    @Override
    public APIResult update(String entityType, String entityName, String filePath,
                            Boolean skipDryRun, String doAsUser) {
        try {
            return localSchedulableEntityManager.update(entityType, entityName, filePath,
                    skipDryRun, "local", doAsUser);
        } catch (FalconException e) {
            throw new FalconCLIException(e);
        }
    }

    @Override
    public Entity getDefinition(String entityType, String entityName, String doAsUser) {
        String entity = localSchedulableEntityManager.getEntityDefinition(entityType, entityName);
        return Entity.fromString(EntityType.getEnum(entityType), entity);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @Override
    public InstancesResult getStatusOfInstances(String type, String entity, String start, String end, String colo,
                                                List<LifeCycle> lifeCycles, String filterBy, String orderBy,
                                                String sortOrder, Integer offset, Integer numResults, String doAsUser,
                                                Boolean allAttempts) {
        if (orderBy == null) {
            orderBy = DEFAULT_ORDER_BY;
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
                sortOrder, offset, numResults, allAttempts);

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
                              String cluster, Boolean skipDryRun, String properties) {
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
                null, null, null, null, null, null, null, null);
        if (instancesResult.getInstances() != null && instancesResult.getInstances().length > 0
                && instancesResult.getInstances()[0] != null) {
            LOG.info("Instance status is " + instancesResult.getInstances()[0].getStatus());
            return instancesResult.getInstances()[0].getStatus();
        }
        return null;
    }

    @Override
    public APIResult suspend(EntityType entityType, String entityName, String colo, String doAsUser) {
        return localSchedulableEntityManager.suspend(entityType.name(), entityName, colo);
    }

    @Override
    public APIResult resume(EntityType entityType, String entityName, String colo, String doAsUser) {
        return localSchedulableEntityManager.resume(entityType.name(), entityName, colo);
    }

    @Override
    public APIResult getStatus(EntityType entityType, String entityName, String colo, String doAsUser,
                               boolean showScheduler) {
        return localSchedulableEntityManager.getStatus(entityType.name(), entityName, colo, showScheduler);
    }

    @Override
    public APIResult submitAndSchedule(String entityType, String filePath, Boolean skipDryRun, String doAsUser,
                                       String properties) {
        try {
            return localSchedulableEntityManager.submitAndSchedule(entityType, filePath, skipDryRun, doAsUser,
                    properties);
        } catch (FalconException | IOException e) {
            throw new FalconCLIException(e);
        }
    }

    @Override
    public APIResult registerExtension(String extensionName, String packagePath, String description, String doAsUser) {
        return localExtensionManager.registerExtensionMetadata(extensionName, packagePath, description);
    }

    @Override
    public APIResult unregisterExtension(String extensionName, String doAsUser) {
        try {
            return localExtensionManager.unRegisterExtension(extensionName);
        } catch (FalconException e) {
            throw new FalconCLIException("Failed in unRegistering the extension"+ e.getMessage());
        }
    }

    @Override
    public APIResult enableExtension(String extensionName, String doAsUser) {
        return localExtensionManager.enableExtension(extensionName);
    }

    @Override
    public APIResult disableExtension(String extensionName, String doAsUser) {
        return localExtensionManager.disableExtension(extensionName);
    }

    @Override
    public APIResult submitExtensionJob(String extensionName, String jobName, String configPath, String doAsUser) {

        InputStream configStream = getServletInputStream(configPath);
        try {
            SortedMap<EntityType, List<Entity>> entityMap = getEntityTypeListMap(extensionName, jobName, configStream);
            return localExtensionManager.submitExtensionJob(extensionName, jobName, configStream, entityMap);
        } catch (FalconException | IOException e) {
            throw new FalconCLIException("Failed in submitting extension job " + jobName);
        }
    }

    private SortedMap<EntityType, List<Entity>> getEntityTypeListMap(String extensionName, String jobName,
                                                                     InputStream configStream) {
        List<Entity> entities = getEntities(extensionName, jobName, configStream);
        List<Entity> feeds = new ArrayList<>();
        List<Entity> processes = new ArrayList<>();
        for (Entity entity : entities) {
            if (EntityType.FEED.equals(entity.getEntityType())) {
                feeds.add(entity);
            } else if (EntityType.PROCESS.equals(entity.getEntityType())) {
                processes.add(entity);
            }
        }
        SortedMap<EntityType, List<Entity>> entityMap = new TreeMap<>();
        entityMap.put(EntityType.PROCESS, processes);
        entityMap.put(EntityType.FEED, feeds);
        return entityMap;
    }

    private List<Entity> getEntities(String extensionName, String jobName, InputStream configStream) {
        String packagePath = ExtensionStore.getMetaStore().getDetail(extensionName).getLocation();
        List<Entity> entities;
        try {
            entities = ExtensionHandler.loadAndPrepare(extensionName, jobName, configStream,
                    packagePath);
        } catch (FalconException | IOException | URISyntaxException e) {
            throw new FalconCLIException("Failed in generating entities for job:" + jobName);
        }
        return entities;
    }

    @Override
    public APIResult scheduleExtensionJob(String jobName, String coloExpr, String doAsUser) {
        try {
            return localExtensionManager.scheduleExtensionJob(jobName, coloExpr, doAsUser);
        } catch (FalconException | IOException e) {
            throw new FalconCLIException("Failed to delete the extension job:" + coloExpr);
        }
    }

    @Override
    public APIResult submitAndScheduleExtensionJob(String extensionName, String jobName, String configPath,
                                                   String doAsUser) {
        InputStream configStream = getServletInputStream(configPath);
        try {
            SortedMap<EntityType, List<Entity>> entityMap = getEntityTypeListMap(extensionName, jobName, configStream);
            return localExtensionManager.submitAndSchedulableExtensionJob(extensionName, jobName, configStream,
                    entityMap);
        } catch (FalconException | IOException e) {
            throw new FalconCLIException("Failed in submitting extension job " + jobName);
        }
    }

    @Override
    public APIResult updateExtensionJob(String jobName, String configPath, String doAsUser) {
        InputStream configStream = getServletInputStream(configPath);
        try {
            String extensionName = ExtensionStore.getMetaStore().getExtensionJobDetails(jobName).getExtensionName();
            SortedMap<EntityType, List<Entity>> entityMap = getEntityTypeListMap(extensionName, jobName, configStream);
            return localExtensionManager.updateExtensionJob(extensionName, jobName, configStream,
                    entityMap);
        } catch (FalconException | IOException e) {
            throw new FalconCLIException("Failed in updating the extension job:" + jobName);
        }
    }

    @Override
    public APIResult deleteExtensionJob(String jobName, String doAsUser) {
        try {
            return localExtensionManager.deleteExtensionJob(jobName);
        } catch (FalconException | IOException e) {
            throw new FalconCLIException("Failed to delete the extension job:" + jobName);
        }
    }

    @Override
    public APIResult suspendExtensionJob(String jobName, String coloExpr, String doAsUser) {
        try {
            return localExtensionManager.suspendExtensionJob(jobName, coloExpr, doAsUser);
        } catch (FalconException e) {
            throw new FalconCLIException("Failed in suspending the extension job:" + jobName);
        }
    }

    @Override
    public APIResult resumeExtensionJob(String jobName, String coloExpr, String doAsUser) {
        try {
            return localExtensionManager.resumeExtensionJob(jobName, coloExpr, doAsUser);
        } catch (FalconException e) {
            throw new FalconCLIException("Failed in resuming the extension job:" + jobName);
        }
    }

    @Override
    public APIResult getExtensionJobDetails(final String jobName, final String doAsUser) {
        return localExtensionManager.getExtensionJobDetails(jobName);
    }

    @Override
    public APIResult getExtensionDetail(String extensionName, String doAsUser) {
        return localExtensionManager.getExtensionDetails(extensionName);
    }

    @Override
    public APIResult enumerateExtensions(String doAsUser) {
        return localExtensionManager.getExtensions();
    }

    @Override
    public ExtensionJobList getExtensionJobs(String extensionName, String sortOrder, String doAsUser) {
        return localExtensionManager.getExtensionJobs(extensionName, sortOrder, doAsUser);
    }

    @Override
    public EntityList getEntityList(String entityType, String fields, String nameSubsequence, String tagKeywords,
                                    String filterBy, String filterTags, String orderBy, String sortOrder,
                                    Integer offset, Integer numResults, String doAsUser) {
        return localSchedulableEntityManager.getEntityList(fields, nameSubsequence, tagKeywords, entityType, filterTags,
                filterBy, orderBy, sortOrder, offset, numResults, doAsUser);
    }

    @Override
    public EntitySummaryResult getEntitySummary(String entityType, String cluster, String start, String end,
                                                String fields, String filterBy, String filterTags, String orderBy,
                                                String sortOrder, Integer offset, Integer numResults,
                                                Integer numInstances, String doAsUser) {
        return localSchedulableEntityManager.getEntitySummary(entityType, cluster, start, end, fields, filterBy,
                filterTags, orderBy, sortOrder, offset, numResults, numInstances, doAsUser);
    }

    @Override
    public APIResult touch(String entityType, String entityName, String colo, Boolean skipDryRun,
                           String doAsUser) {
        return localSchedulableEntityManager.touch(entityType, entityName, colo, skipDryRun);
    }

    public InstancesResult killInstances(String type, String entity, String start, String end, String colo,
                                         String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                         String doAsUser) throws UnsupportedEncodingException {
        Properties props = getProperties(clusters, sourceClusters);
        return localInstanceManager.killInstance(props, type, entity, start, end, colo, lifeCycles);
    }

    public InstancesResult suspendInstances(String type, String entity, String start, String end, String colo,
                                            String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                            String doAsUser) throws UnsupportedEncodingException {
        Properties props = getProperties(clusters, sourceClusters);
        return localInstanceManager.suspendInstance(props, type, entity, start, end, colo, lifeCycles);
    }

    public InstancesResult resumeInstances(String type, String entity, String start, String end, String colo,
                                           String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                           String doAsUser) throws UnsupportedEncodingException {
        Properties props = getProperties(clusters, sourceClusters);
        return localInstanceManager.resumeInstance(props, type, entity, start, end, colo, lifeCycles);
    }

    public InstancesResult rerunInstances(String type, String entity, String start, String end, String filePath,
                                          String colo, String clusters, String sourceClusters,
                                          List<LifeCycle> lifeCycles, Boolean isForced, String doAsUser) throws
            IOException {
        Properties props = getProperties(clusters, sourceClusters);
        return localInstanceManager.reRunInstance(type, entity, start, end, props, colo, lifeCycles, isForced);
    }

    public InstancesSummaryResult getSummaryOfInstances(String type, String entity, String start, String end,
                                                        String colo, List<LifeCycle> lifeCycles, String filterBy,
                                                        String orderBy, String sortOrder, String doAsUser) {
        if (StringUtils.isBlank(orderBy)) {
            orderBy = DEFAULT_ORDER_BY;
        }
        if (StringUtils.isBlank(sortOrder)) {
            sortOrder = DEFAULT_SORTED_ORDER;
        }
        return localInstanceManager.getSummary(type, entity, start, end, colo, lifeCycles, filterBy, orderBy,
                sortOrder);
    }

    public FeedInstanceResult getFeedListing(String type, String entity, String start, String end, String colo,
                                             String doAsUser) {
        return localInstanceManager.getListing(type, entity, start, end, colo);
    }

    public InstancesResult getLogsOfInstances(String type, String entity, String start, String end, String colo,
                                              String runId, List<LifeCycle> lifeCycles, String filterBy,
                                              String orderBy, String sortOrder, Integer offset, Integer numResults,
                                              String doAsUser) {
        return localInstanceManager.getLogs(type, entity, start, end, colo, runId, lifeCycles, filterBy, orderBy,
                sortOrder, offset, numResults);
    }

    public InstancesResult getParamsOfInstance(String type, String entity, String start, String colo,
                                               List<LifeCycle> lifeCycles, String doAsUser) throws
            UnsupportedEncodingException {
        return localInstanceManager.getInstanceParams(type, entity, start, colo, lifeCycles);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    public InstanceDependencyResult getInstanceDependencies(String entityType, String entityName, String instanceTime,
                                                            String colo) {
        return localInstanceManager.getInstanceDependencies(entityType, entityName, instanceTime, colo);
    }

    @Override
    public String getVersion(String doAsUser) {
        AdminResource resource = new AdminResource();
        AdminResource.PropertyList propertyList = resource.getVersion();
        Map<String, String> version = new LinkedHashMap<>();
        List<String> list = new ArrayList<>();
        for (AdminResource.Property property : propertyList.properties) {
            Map<String, String> map = new LinkedHashMap<>();
            map.put("key", property.key);
            map.put("value", property.value);
            list.add(JSONValue.toJSONString(map));
        }
        version.put("properties", list.toString());
        return version.toString();
    }

    @Override
    public SchedulableEntityInstanceResult getFeedSlaMissPendingAlerts(String entityType, String entityName,
                                                                       String start, String end, String colo) {
        return null;
    }

    @Override
    public FeedLookupResult reverseLookUp(String entityType, String path, String doAs) {
        return null;
    }

    @Override
    public EntityList getDependency(String entityType, String entityName, String doAs) {
        return null;
    }

    @Override
    public TriageResult triage(String name, String entityName, String start, String colo) {
        return null;
    }
    // SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @Override
    public InstancesResult getRunningInstances(String type, String entity, String colo, List<LifeCycle> lifeCycles,
                                               String filterBy, String orderBy, String sortOrder,
                                               Integer offset, Integer numResults, String doAsUser) {
        return null;
    }
    // RESUME CHECKSTYLE CHECK ParameterNumberCheck
    @Override
    public FeedInstanceResult getFeedInstanceListing(String type, String entity, String start, String end,
                                                     String colo, String doAsUser) {
        return null;
    }

    @Override
    public int getStatus(String doAsUser) {
        return 200;
    }

    @Override
    public String getThreadDump(String doAs) {
        return "";
    }

    @Override
    public LineageGraphResult getEntityLineageGraph(String pipeline, String doAs) {
        return null;
    }

    @Override
    public String getDimensionList(String dimensionType, String cluster, String doAs) {
        return null;
    }

    @Override
    public String getReplicationMetricsDimensionList(String schedEntityType, String schedEntityName,
                                                     Integer numResults, String doAs) {
        return null;
    }

    @Override
    public String getDimensionRelations(String dimensionType, String dimensionName, String doAs) {
        return null;
    }

    @Override
    public String getVertex(String id, String doAs) {
        return null;
    }

    @Override
    public String getVertices(String key, String value, String doAs) {
        return null;
    }

    @Override
    public String getVertexEdges(String id, String direction, String doAs) {
        return null;
    }

    @Override
    public String getEdge(String id, String doAs) {
        return null;
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
