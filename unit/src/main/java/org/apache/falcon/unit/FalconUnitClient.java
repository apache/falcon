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
import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Client for Falcon Unit.
 */
public class FalconUnitClient extends AbstractFalconClient {

    private static final Logger LOG = LoggerFactory.getLogger(FalconUnitClient.class);

    protected ConfigurationStore configStore;
    private AbstractWorkflowEngine workflowEngine;

    public FalconUnitClient() throws FalconException {
        configStore = ConfigurationStore.get();
        workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
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
            EntityType entityType = EntityType.getEnum(type);
            InputStream entityStream = FalconUnitHelper.getFileInputStream(filePath);
            EntityParser entityParser = EntityParserFactory.getParser(entityType);
            Entity entity = entityParser.parse(entityStream);

            Entity existingEntity = configStore.get(entityType, entity.getName());
            if (existingEntity != null) {
                if (EntityUtil.equals(existingEntity, entity)) {
                    LOG.warn(entity.toShortString() + " already registered with same definition " + entity.getName());
                    return new APIResult(APIResult.Status.SUCCEEDED, "{} already registered with same definition"
                            + entity.getName());
                }
                LOG.warn(entity.toShortString() + " already registered with different definition "
                        + "Can't be submitted again. Try removing before submitting.");
                return new APIResult(APIResult.Status.FAILED, "{} already registered with different definition "
                        + "Can't be submitted again. Try removing before submitting." + entity.getName());
            }

            entityParser.validate(entity);
            configStore.publish(entityType, entity);
            LOG.info("Submit successful: ({}): {}", entityType.name(), entity.getName());
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + type + ") " + entity.getName());
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
                              Boolean skipDryRun, String doAsUser) throws FalconCLIException {
        return schedule(entityType, entityName, null, 0, cluster, skipDryRun);
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
                              String cluster, Boolean skipDryRun) throws FalconCLIException {
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
            workflowEngine.schedule(entity, skipDryRun);
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
    public InstancesResult.WorkflowStatus getInstanceStatus(EntityType entityType, String entityName,
                                                            String nominalTime) throws Exception {
        if (entityType == EntityType.CLUSTER) {
            throw new IllegalArgumentException("Instance management functions don't apply to Cluster entities");
        }
        Entity entityObject = EntityUtil.getEntity(entityType, entityName);
        Date startTime = SchemaHelper.parseDateUTC(nominalTime);
        Date endTime = DateUtil.getNextMinute(startTime);
        List<LifeCycle> lifeCycles = FalconUnitHelper.checkAndUpdateLifeCycle(null, entityType.name());
        InstancesResult instancesResult = workflowEngine.getStatus(entityObject, startTime, endTime, lifeCycles);
        if (instancesResult.getInstances() != null && instancesResult.getInstances().length > 0
                && instancesResult.getInstances()[0] != null) {
            LOG.info("Instance status is " + instancesResult.getInstances()[0].getStatus());
            return instancesResult.getInstances()[0].getStatus();
        }
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
}

