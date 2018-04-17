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

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.lock.MemoryLocks;
import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.store.EntityAlreadyExistsException;
import org.apache.falcon.entity.store.FeedLocationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityGraph;
import org.apache.falcon.entity.v0.EntityIntegrityChecker;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.resource.APIResult.Status;
import org.apache.falcon.resource.EntityList.EntityElement;
import org.apache.falcon.resource.metadata.AbstractMetadataResource;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.DefaultAuthorizationProvider;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A base class for managing Entity operations.
 */
public abstract class AbstractEntityManager extends AbstractMetadataResource {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntityManager.class);
    private static MemoryLocks memoryLocks = MemoryLocks.getInstance();
    protected static final String DO_AS_PARAM = "doAs";

    protected static final int XML_DEBUG_LEN = 10 * 1024;
    protected ConfigurationStore configStore = ConfigurationStore.get();

    public AbstractEntityManager() {
    }

    protected static Integer getDefaultResultsPerPage() {
        Integer result = 10;
        final String key = "webservices.default.results.per.page";
        String value = RuntimeProperties.get().getProperty(key, result.toString());
        try {
            result = Integer.valueOf(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid value:{} for key:{} in runtime.properties", value, key);
        }
        return result;
    }

    protected static void checkColo(String colo) {
        if (DeploymentUtil.isEmbeddedMode()) {
            return;
        }
        if (StringUtils.isNotEmpty(colo) && !colo.equals("*")) {
            if (!DeploymentUtil.getCurrentColo().equals(colo)) {
                throw FalconWebException.newAPIException("Current colo (" + DeploymentUtil.getCurrentColo()
                        + ") is not " + colo);
            }
        }
    }

    public static Set<String> getAllColos() {
        if (DeploymentUtil.isEmbeddedMode()) {
            return DeploymentUtil.getDefaultColos();
        }
        String[] colos = RuntimeProperties.get().getProperty("all.colos", DeploymentUtil.getDefaultColo()).split(",");
        for (int i = 0; i < colos.length; i++) {
            colos[i] = colos[i].trim();
        }
        return new HashSet<String>(Arrays.asList(colos));
    }

    protected Set<String> getColosFromExpression(String coloExpr, String type, String entity) {
        final Set<String> applicableColos = getApplicableColos(type, entity);
        return getColosFromExpression(coloExpr, applicableColos);
    }

    protected Set<String> getColosFromExpression(String coloExpr, String type, Entity entity) {
        final Set<String> applicableColos = getApplicableColos(type, entity);
        return getColosFromExpression(coloExpr, applicableColos);
    }

    private Set<String> getColosFromExpression(String coloExpr, Set<String> applicableColos) {
        Set<String> colos;
        if (coloExpr == null || coloExpr.equals("*") || coloExpr.isEmpty()) {
            colos = applicableColos;
        } else {
            colos = new HashSet<>(Arrays.asList(coloExpr.split(",")));
            if (!applicableColos.containsAll(colos)) {
                throw FalconWebException.newAPIException("Given colos not applicable for entity operation");
            }
        }
        return colos;
    }

    public static Set<String> getApplicableColos(String type, String name) {
        try {
            if (DeploymentUtil.isEmbeddedMode()) {
                return DeploymentUtil.getDefaultColos();
            }

            if (EntityType.getEnum(type) == EntityType.CLUSTER || name == null) {
                return getAllColos();
            }

            return getApplicableColos(type, EntityUtil.getEntity(type, name));
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e);
        }
    }

    public static Set<String> getApplicableColos(String type, Entity entity) {
        try {
            if (DeploymentUtil.isEmbeddedMode()) {
                return DeploymentUtil.getDefaultColos();
            }

            if (EntityType.getEnum(type) == EntityType.CLUSTER) {
                return getAllColos();
            }

            Set<String> clusters = EntityUtil.getClustersDefined(entity);
            Set<String> colos = new HashSet<String>();
            for (String cluster : clusters) {
                Cluster clusterEntity = EntityUtil.getEntity(EntityType.CLUSTER, cluster);
                colos.add(clusterEntity.getColo());
            }
            return colos;
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e);
        }
    }

    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system
     * <p/>
     * Entity name acts as the key and an entity once added, can't be added
     * again unless deleted.
     *
     * @param request - Servlet Request
     * @param type    - entity type - feed, process or data end point
     * @param colo    - applicable colo
     * @return result of the operation
     */
    public APIResult submit(HttpServletRequest request, String type, String colo) {

        checkColo(colo);
        try {
            String doAsUser = request.getParameter(DO_AS_PARAM);
            Entity entity = submitInternal(request.getInputStream(), type, doAsUser);
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + type + ") " + entity.getName());
        } catch (Throwable e) {
            LOG.error("Unable to persist entity object", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    /**
     * Post an entity XML with entity type. Validates the XML which can be
     * Process, Feed or Data endpoint
     *
     * @param type entity type
     * @return APIResult -Succeeded or Failed
     */
    public APIResult validate(HttpServletRequest request, String type, Boolean skipDryRun) {
        try {
            return validate(request.getInputStream(), type, skipDryRun);
        } catch (IOException e) {
            LOG.error("Unable to get InputStream from Request", request, e);
            throw FalconWebException.newAPIException(e);
        }
    }

    protected APIResult validate(InputStream inputStream, String type, Boolean skipDryRun) {
        try {
            EntityType entityType = EntityType.getEnum(type);
            Entity entity = deserializeEntity(inputStream, entityType);
            validate(entity);

            // Validate that the entity can be scheduled in the cluster.
            // Perform dryrun only if falcon is not in safemode.
            if (entity.getEntityType().isSchedulable() && !StartupProperties.isServerInSafeMode()) {
                Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
                for (String cluster : clusters) {
                    try {
                        getWorkflowEngine(entity).dryRun(entity, cluster, skipDryRun);
                    } catch (FalconException e) {
                        throw new FalconException("dryRun failed on cluster " + cluster, e);
                    }
                }
            }
            return new APIResult(APIResult.Status.SUCCEEDED,
                    "Validated successfully (" + entityType + ") " + entity.getName());
        } catch (Throwable e) {
            LOG.error("Validation failed for entity ({})", type, e);
            throw FalconWebException.newAPIException(e);
        }
    }

    /**
     * Deletes a scheduled entity, a deleted entity is removed completely from
     * execution pool.
     *
     * @param type   entity type
     * @param entity entity name
     * @return APIResult
     */
    public APIResult delete(HttpServletRequest request, String type, String entity, String colo) {
        return delete(type, entity, colo);

    }

    protected APIResult delete(String type, String entity, String colo) {
        checkColo(colo);
        List<Entity> tokenList = new ArrayList<>();
        try {
            EntityType entityType = EntityType.getEnum(type);
            String removedFromEngine = "";
            try {
                Entity entityObj = EntityUtil.getEntity(type, entity);
                verifySafemodeOperation(entityObj, EntityUtil.ENTITY_OPERATION.DELETE);

                canRemove(entityObj);
                obtainEntityLocks(entityObj, "delete", tokenList);
                if (entityType.isSchedulable() && !DeploymentUtil.isPrism()) {
                    getWorkflowEngine(entityObj).delete(entityObj);
                    removedFromEngine = "(KILLED in WF_ENGINE)";
                }

                configStore.remove(entityType, entity);
            } catch (EntityNotRegisteredException e) { // already deleted
                return new APIResult(APIResult.Status.SUCCEEDED,
                        entity + "(" + type + ") doesn't exist. Nothing to do");
            }

            return new APIResult(APIResult.Status.SUCCEEDED,
                    entity + "(" + type + ") removed successfully " + removedFromEngine);
        } catch (Throwable e) {
            LOG.error("Unable to reach workflow engine for deletion or deletion failed", e);
            throw FalconWebException.newAPIException(e);
        } finally {
            releaseEntityLocks(entity, tokenList);
        }
    }

    public APIResult update(HttpServletRequest request, String type, String entityName,
                            String colo, Boolean skipDryRun) {
        try {
            return update(request.getInputStream(), type, entityName, colo, skipDryRun);
        } catch (IOException e) {
            LOG.error("Unable to get InputStream from Request", request, e);
            throw FalconWebException.newAPIException(e);
        }

    }

    protected APIResult update(InputStream inputStream, String type, String entityName,
                               String colo, Boolean skipDryRun) {
        checkColo(colo);
        try {
            EntityType entityType = EntityType.getEnum(type);
            Entity entity = deserializeEntity(inputStream, entityType);
            verifySafemodeOperation(entity, EntityUtil.ENTITY_OPERATION.UPDATE);
            return update(entity, type, entityName, skipDryRun);
        } catch (FalconException e) {
            LOG.error("Update failed", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    protected APIResult update(Entity newEntity, String type, String entityName, Boolean skipDryRun) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            EntityType entityType = EntityType.getEnum(type);
            Entity oldEntity = EntityUtil.getEntity(type, entityName);
            // KLUDGE - Until ACL is mandated entity passed should be decorated for equals check to pass
            decorateEntityWithACL(newEntity);
            validate(newEntity);

            validateUpdate(oldEntity, newEntity);
            configStore.initiateUpdate(newEntity);
            obtainEntityLocks(oldEntity, "update", tokenList);

            StringBuilder result = new StringBuilder("Updated successfully");
            switch(entityType) {
            case CLUSTER:
                configStore.update(entityType, newEntity);
                break;
            case DATASOURCE:
                configStore.update(entityType, newEntity);
                // check always if dependant feeds are already upgraded and upgrade accordingly
                if (entityType.equals(EntityType.DATASOURCE)) {
                    ConfigurationStore.get().cleanupUpdateInit();
                    releaseEntityLocks(entityName, tokenList);
                    updateDatasourceDependents(entityName, skipDryRun);
                }
                break;
            case FEED:
            case PROCESS:
                if (!DeploymentUtil.isPrism()) {
                    Set<String> oldClusters = EntityUtil.getClustersDefinedInColos(oldEntity);
                    Set<String> newClusters = EntityUtil.getClustersDefinedInColos(newEntity);
                    newClusters.retainAll(oldClusters); //common clusters for update
                    oldClusters.removeAll(newClusters); //deleted clusters
                    for (String cluster : newClusters) {
                        result.append(getWorkflowEngine(oldEntity).update(oldEntity, newEntity, cluster, skipDryRun));
                    }
                    for (String cluster : oldClusters) {
                        getWorkflowEngine(oldEntity).delete(oldEntity, cluster);
                    }
                }
                configStore.update(entityType, newEntity);
                break;
            default:
                throw FalconWebException.newAPIException("Unknown entity type in update : " + entityType);
            }
            return new APIResult(APIResult.Status.SUCCEEDED, result.toString());
        } catch (Throwable e) {
            LOG.error("Update failed", e);
            throw FalconWebException.newAPIException(e);
        } finally {
            ConfigurationStore.get().cleanupUpdateInit();
            releaseEntityLocks(entityName, tokenList);
        }
    }

    /**
     * check if the data source entity dependent feeds are upgraded or not by checking against the data source entity
     * version and upgrade feeds accordingly.
     *
     * @param datasourceName Name of the data source entity
     * @param skipDryRun Skip dry run during update if set to true
     * @return APIResult
     *
     */

    public APIResult updateDatasourceDependents(String datasourceName, Boolean skipDryRun) {
        try {
            Datasource datasource = EntityUtil.getEntity(EntityType.DATASOURCE, datasourceName);
            StringBuilder result = new StringBuilder(String.format("Updating feed entities "
                    + "dependent on datasource : %s ", datasource.getName()));

            // get data source dependent entities and check the version referenced is same
            Pair<String, EntityType>[] dependentEntities = EntityIntegrityChecker.referencedBy(datasource);
            if (dependentEntities == null) {
                return new APIResult(APIResult.Status.SUCCEEDED, String.format("Datasource %s has "
                        + "no dependent entities", datasourceName));
            }
            for (Pair<String, EntityType> depEntity : dependentEntities) {
                Entity entity = EntityUtil.getEntity(depEntity.second, depEntity.first);
                if (entity.getEntityType() != EntityType.FEED) {
                    throw FalconWebException.newAPIException("Datasource dependents should be FEEDS, but"
                        + "encountered type : " + entity.getEntityType());
                }
                Feed newFeed = (Feed) entity.copy();
                for (org.apache.falcon.entity.v0.feed.Cluster feedCluster
                        : newFeed.getClusters().getClusters()) {
                    if (feedCluster.getType() == ClusterType.SOURCE) {
                        boolean updatedFeed = isUpdateFeedDatasourceVersion(feedCluster, datasource, newFeed);
                        if (updatedFeed) {
                            // rewrite the dependent feed and update it on the store
                            result.append(getWorkflowEngine(entity).update(entity, newFeed,
                                    feedCluster.getName(), skipDryRun));
                            updateEntityInConfigStore(entity, newFeed);
                        }
                    }
                }
            }
            return new APIResult(APIResult.Status.SUCCEEDED, result.toString());
        } catch (FalconException e) {
            LOG.error("Update failed", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private boolean isUpdateFeedDatasourceVersion(org.apache.falcon.entity.v0.feed.Cluster feedCluster,
        Datasource datasource, Feed feed) throws FalconException {
        org.apache.falcon.entity.v0.feed.Datasource updateFeedImp = incFeedDatasourceVersion(datasource,
                feed, feedCluster.getImport() != null ? feedCluster.getImport().getSource() : null);
        org.apache.falcon.entity.v0.feed.Datasource updateFeedExp = incFeedDatasourceVersion(datasource,
                feed, feedCluster.getExport() != null ? feedCluster.getExport().getTarget() : null);
        return ((updateFeedImp != null) || (updateFeedExp != null));
    }

    private  org.apache.falcon.entity.v0.feed.Datasource  incFeedDatasourceVersion(Datasource datasource,
        Feed feed, org.apache.falcon.entity.v0.feed.Datasource depDatasource) throws FalconException {
        if ((depDatasource != null) && (datasource.getName().equals(depDatasource.getName()))) {
            if (depDatasource.getVersion() < datasource.getVersion()) {
                LOG.info(String.format("Updating since Feed '%s' referenced datasource '%s' "
                        + "version '%d' < datasource entity version in store '%d'", feed.getName(),
                        depDatasource.getName(), depDatasource.getVersion(), datasource.getVersion()));
                depDatasource.setVersion(depDatasource.getVersion()+1);
                return depDatasource;
            } else if (depDatasource.getVersion() > datasource.getVersion()) {
                throw new FalconException(String.format("Feed '%s' datasource '%s' version '%d' > datasource "
                        + "entity version in store '%d'", feed.getName(), depDatasource.getName(),
                        depDatasource.getVersion(), datasource.getVersion()));
            }
        }
        return null;
    }


    /**
     * Updates scheduled dependent entities of a cluster.
     *
     * @param clusterName   Name of cluster
     * @param colo colo
     * @param skipDryRun Skip dry run during update if set to true
     * @return APIResult
     */
    public APIResult updateClusterDependents(String clusterName, String colo, Boolean skipDryRun) {
        checkColo(colo);
        try {
            verifySuperUser();
            Cluster cluster = EntityUtil.getEntity(EntityType.CLUSTER, clusterName);
            verifySafemodeOperation(cluster, EntityUtil.ENTITY_OPERATION.UPDATE_CLUSTER_DEPENDENTS);
            int clusterVersion = cluster.getVersion();
            StringBuilder result = new StringBuilder("Updating entities dependent on cluster \n");
            // get dependent entities. check if cluster version changed. if yes, update dependent entities
            Pair<String, EntityType>[] dependentEntities = EntityIntegrityChecker.referencedBy(cluster);
            if (dependentEntities == null) {
                // nothing to update
                return new APIResult(APIResult.Status.SUCCEEDED, "Cluster "
                        + clusterName + " has no dependent entities");
            }
            for (Pair<String, EntityType> depEntity : dependentEntities) {
                Entity entity = EntityUtil.getEntity(depEntity.second, depEntity.first);
                switch (entity.getEntityType()) {
                case FEED:
                    Feed newFeedEntity = (Feed) entity.copy();
                    Clusters feedClusters = newFeedEntity.getClusters();
                    if (feedClusters != null) {
                        boolean requireUpdate = false;
                        for(org.apache.falcon.entity.v0.feed.Cluster feedCluster : feedClusters.getClusters()) {
                            if (feedCluster.getName().equals(clusterName)
                                    && feedCluster.getVersion() != clusterVersion) {
                                // update feed cluster entity
                                feedCluster.setVersion(clusterVersion);
                                requireUpdate = true;
                            }
                        }
                        if (requireUpdate) {
                            result.append(getWorkflowEngine(entity).update(entity, newFeedEntity,
                                    cluster.getName(), skipDryRun));
                            updateEntityInConfigStore(entity, newFeedEntity);
                        }
                    }
                    break;
                case PROCESS:
                    Process newProcessEntity = (Process) entity.copy();
                    org.apache.falcon.entity.v0.process.Clusters processClusters = newProcessEntity.getClusters();
                    if (processClusters != null) {
                        boolean requireUpdate = false;
                        for(org.apache.falcon.entity.v0.process.Cluster procCluster : processClusters.getClusters()) {
                            if (procCluster.getName().equals(clusterName)
                                    && procCluster.getVersion() != clusterVersion) {
                                // update feed cluster entity
                                procCluster.setVersion(clusterVersion);
                                requireUpdate = true;
                            }
                        }
                        if (requireUpdate) {
                            result.append(getWorkflowEngine(entity).update(entity, newProcessEntity,
                                    cluster.getName(), skipDryRun));
                            updateEntityInConfigStore(entity, newProcessEntity);
                        }
                    }
                    break;
                default:
                    break;
                }
            }
            return new APIResult(APIResult.Status.SUCCEEDED, result.toString());
        } catch (Exception e) {
            LOG.error("Update failed", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private void updateEntityInConfigStore(Entity oldEntity, Entity newEntity) {
        List<Entity> tokenList = new ArrayList<>();
        try {
            configStore.initiateUpdate(newEntity);
            obtainEntityLocks(oldEntity, "update", tokenList);
            configStore.update(newEntity.getEntityType(), newEntity);
        } catch (Throwable e) {
            LOG.error("Update failed", e);
            throw FalconWebException.newAPIException(e);
        } finally {
            ConfigurationStore.get().cleanupUpdateInit();
            releaseEntityLocks(oldEntity.getName(), tokenList);
        }

    }

    private void obtainEntityLocks(Entity entity, String command, List<Entity> tokenList)
        throws FalconException {
        //first obtain lock for the entity for which update is issued.
        if (memoryLocks.acquireLock(entity, command)) {
            tokenList.add(entity);
        } else {
            throw new FalconException(command + " command is already issued for " + entity.toShortString());
        }

        //now obtain locks for all dependent entities if any.
        Set<Entity> affectedEntities = EntityGraph.get().getDependents(entity);
        for (Entity e : affectedEntities) {
            if (e.getEntityType() != EntityType.CLUSTER) {
                if (memoryLocks.acquireLock(e, command)) {
                    tokenList.add(e);
                    LOG.debug("{} on entity {} has acquired lock on {}", command, entity, e);
                } else {
                    LOG.error("Error while trying to acquire lock on {}. Releasing already obtained locks",
                            e.toShortString());
                    throw new FalconException("There are multiple update commands running for dependent entity "
                            + e.toShortString());
                }
            }
        }
    }

    private void releaseEntityLocks(String entityName, List<Entity> tokenList) {
        if (tokenList != null && !tokenList.isEmpty()) {
            for (Entity entity : tokenList) {
                memoryLocks.releaseLock(entity);
            }
            LOG.info("All locks released on {}", entityName);
        } else {
            LOG.info("No locks to release on " + entityName);
        }

    }

    private void validateUpdate(Entity oldEntity, Entity newEntity) throws FalconException, IOException {
        if (oldEntity.getEntityType() != newEntity.getEntityType() || !oldEntity.equals(newEntity)) {
            throw new FalconException(
                    oldEntity.toShortString() + " can't be updated with " + newEntity.toShortString());
        }

        if (oldEntity.getEntityType() == EntityType.CLUSTER) {
            verifySuperUser();
        }

        String[] props = oldEntity.getEntityType().getImmutableProperties();
        for (String prop : props) {
            Object oldProp, newProp;
            try {
                oldProp = PropertyUtils.getProperty(oldEntity, prop);
                newProp = PropertyUtils.getProperty(newEntity, prop);
            } catch (Exception e) {
                throw new FalconException(e);
            }
            if (!ObjectUtils.equals(oldProp, newProp)) {
                throw new ValidationException(oldEntity.toShortString() + ": " + prop + " can't be changed");
            }
        }
    }

    protected void canRemove(Entity entity) throws FalconException {
        Pair<String, EntityType>[] referencedBy = EntityIntegrityChecker.referencedBy(entity);
        if (referencedBy != null && referencedBy.length > 0) {
            StringBuilder messages = new StringBuilder();
            for (Pair<String, EntityType> ref : referencedBy) {
                messages.append(ref).append("\n");
            }
            throw new FalconException(
                    entity.getName() + "(" + entity.getEntityType() + ") cant " + "be removed as it is referred by "
                            + messages);
        }
    }

    protected Entity submitInternal(InputStream inputStream, String type, String doAsUser)
        throws IOException, FalconException {
        EntityType entityType = EntityType.getEnum(type);
        Entity entity = deserializeEntity(inputStream, entityType);
        verifySafemodeOperation(entity, EntityUtil.ENTITY_OPERATION.SUBMIT);
        submitInternal(entity, doAsUser);
        return entity;
    }

    protected void verifySafemodeOperation(Entity entity, EntityUtil.ENTITY_OPERATION operation) {
        // if Falcon not in safemode, allow everything except cluster update
        if (!StartupProperties.isServerInSafeMode()) {
            if (operation.equals(EntityUtil.ENTITY_OPERATION.UPDATE)
                    && entity.getEntityType().equals(EntityType.CLUSTER)) {
                LOG.error("Entity operation {} is only allowed on cluster entities during safemode",
                        operation.name());
                throw FalconWebException.newAPIException("Entity operation " + operation.name()
                        + " is only allowed on cluster entities during safemode");
            }
            return;
        }

        switch (operation) {
        case UPDATE:
            if (entity.getEntityType().equals(EntityType.CLUSTER)) {
                return;
            } else {
                LOG.error("Entity operation {} is only allowed on cluster entities during safemode",
                        operation.name());
                throw FalconWebException.newAPIException("Entity operation " + operation.name()
                        + " is only allowed on cluster entities during safemode");
            }
        case SUSPEND:
            if (entity.getEntityType().equals(EntityType.CLUSTER)) {
                LOG.error("Entity operation {} is not allowed on cluster entity",
                        operation.name());
                throw FalconWebException.newAPIException("Entity operation " + operation.name()
                        + " is not allowed on cluster entity");
            } else {
                return;
            }
        case SCHEDULE:
        case UPDATE_CLUSTER_DEPENDENTS:
        case SUBMIT_AND_SCHEDULE:
        case DELETE:
        case RESUME:
        case TOUCH:
        case SUBMIT:
        default:
            LOG.error("Entity operation {} is not allowed during safemode", operation.name());
            throw FalconWebException.newAPIException("Entity operation "
                    + operation.name() + " not allowed during safemode");
        }
    }

    protected synchronized void submitInternal(Entity entity, String doAsUser) throws IOException, FalconException {
        EntityType entityType = entity.getEntityType();
        List<Entity> tokenList = new ArrayList<>();
        // KLUDGE - Until ACL is mandated entity passed should be decorated for equals check to pass
        decorateEntityWithACL(entity);

        try {
            obtainEntityLocks(entity, "submit", tokenList);
        }finally {
            ConfigurationStore.get().cleanupUpdateInit();
            releaseEntityLocks(entity.getName(), tokenList);
        }
        Entity existingEntity = configStore.get(entityType, entity.getName());
        if (existingEntity != null) {
            if (EntityUtil.equals(existingEntity, entity)) {
                return;
            }

            throw new EntityAlreadyExistsException(
                    entity.toShortString() + " already registered with configuration store. "
                            + "Can't be submitted again. Try removing before submitting.");
        }

        SecurityUtil.tryProxy(entity, doAsUser); // proxy before validating since FS/Oozie needs to be proxied
        validate(entity);
        configStore.publish(entityType, entity);
        LOG.info("Submit successful: ({}): {}", entityType, entity.getName());
    }

    /**
     * KLUDGE - Until ACL is mandated entity passed should be decorated for equals check to pass.
     * existingEntity in config store will have teh decoration and equals check fails
     * if entity passed is not decorated for checking if entity already exists.
     *
     * @param entity entity
     */
    protected void decorateEntityWithACL(Entity entity) {
        if (SecurityUtil.isAuthorizationEnabled() || entity.getACL() != null) {
            return; // not necessary to decorate
        }

        final String proxyUser = CurrentUser.getUser();
        final String defaultGroupName = CurrentUser.getPrimaryGroupName();
        switch (entity.getEntityType()) {
        case CLUSTER:
            org.apache.falcon.entity.v0.cluster.ACL clusterACL =
                    new org.apache.falcon.entity.v0.cluster.ACL();
            clusterACL.setOwner(proxyUser);
            clusterACL.setGroup(defaultGroupName);
            ((org.apache.falcon.entity.v0.cluster.Cluster) entity).setACL(clusterACL);
            break;

        case FEED:
            org.apache.falcon.entity.v0.feed.ACL feedACL =
                    new org.apache.falcon.entity.v0.feed.ACL();
            feedACL.setOwner(proxyUser);
            feedACL.setGroup(defaultGroupName);
            ((org.apache.falcon.entity.v0.feed.Feed) entity).setACL(feedACL);
            break;

        case PROCESS:
            org.apache.falcon.entity.v0.process.ACL processACL =
                    new org.apache.falcon.entity.v0.process.ACL();
            processACL.setOwner(proxyUser);
            processACL.setGroup(defaultGroupName);
            ((org.apache.falcon.entity.v0.process.Process) entity).setACL(processACL);
            break;

        default:
            break;
        }
    }

    protected Entity deserializeEntity(InputStream xmlStream, EntityType entityType)
        throws FalconException {

        EntityParser<?> entityParser = EntityParserFactory.getParser(entityType);
        if (xmlStream.markSupported()) {
            xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
        }
        try {
            return entityParser.parse(xmlStream);
        } catch (FalconException e) {
            if (LOG.isDebugEnabled() && xmlStream.markSupported()) {
                try {
                    xmlStream.reset();
                    String xmlData = getAsString(xmlStream);
                    LOG.debug("XML DUMP for ({}): {}", entityType, xmlData, e);
                } catch (IOException ignore) {
                    // ignore
                }
            }
            throw e;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void validate(Entity entity) throws FalconException {
        EntityParser entityParser = EntityParserFactory.getParser(entity.getEntityType());
        entityParser.validate(entity);
    }

    private String getAsString(InputStream xmlStream) throws IOException {
        byte[] data = new byte[XML_DEBUG_LEN];
        IOUtils.readFully(xmlStream, data, 0, XML_DEBUG_LEN);
        return new String(data);
    }

    /**
     * Enumeration of all possible status of an entity.
     */
    public enum EntityStatus {
        SUBMITTED, SUSPENDED, RUNNING, COMPLETED
    }

    /**
     * Returns the status of requested entity.
     *
     * @param type  entity type
     * @param entity entity name
     * @param showScheduler whether to return the scheduler on which the entity is scheduled.
     * @return String
     */
    public APIResult getStatus(String type, String entity, String colo, Boolean showScheduler) {

        checkColo(colo);
        Entity entityObj;
        try {
            entityObj = EntityUtil.getEntity(type, entity);
            EntityType entityType = EntityType.getEnum(type);
            Pair<EntityStatus, String> status = getStatus(entityObj, entityType);
            String statusString = status.first.name();
            return new APIResult(Status.SUCCEEDED, (status.first != EntityStatus.SUBMITTED
                    && showScheduler != null && showScheduler)
                    ? statusString + " (scheduled on " + status.second + ")" : statusString);
        } catch (FalconWebException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unable to get status for entity {} ({})", entity, type, e);
            throw FalconWebException.newAPIException(e);
        }
    }

    protected Pair<EntityStatus, String> getStatus(Entity entity, EntityType type) throws FalconException {
        EntityStatus status = EntityStatus.SUBMITTED;
        AbstractWorkflowEngine workflowEngine = getWorkflowEngine(entity);
        if (type.isSchedulable()) {
            if (workflowEngine.isActive(entity)) {
                if (workflowEngine.isSuspended(entity)) {
                    status = EntityStatus.SUSPENDED;
                } else {
                    status = EntityStatus.RUNNING;
                }
            } else if (workflowEngine.isCompleted(entity)) {
                status = EntityStatus.COMPLETED;
            }
        }
        return new Pair<>(status, workflowEngine.getName());
    }

    /**
     * Returns dependencies.
     *
     * @param type entity type
     * @param entityName entity name
     * @return EntityList
     */
    public EntityList getDependencies(String type, String entityName) {

        try {
            Entity entityObj = EntityUtil.getEntity(type, entityName);
            return EntityUtil.getEntityDependencies(entityObj);
        } catch (Exception e) {
            LOG.error("Unable to get dependencies for entityName {} ({})", entityName, type, e);
            throw FalconWebException.newAPIException(e);
        }
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    /**
     * Returns the list of filtered entities as well as the total number of results.
     *
     * @param fieldStr         Fields that the query is interested in, separated by comma
     * @param nameSubsequence  Name subsequence to match
     * @param tagKeywords      Tag keywords to match, separated by commma
     * @param filterType       Only return entities of this type
     * @param filterTags       Full tag matching, separated by comma
     * @param filterBy         Specific fields to match (i.e. TYPE, NAME, STATUS, PIPELINES, CLUSTER)
     * @param orderBy          Order result by these fields.
     * @param sortOrder        Valid options are "asc" and “desc”
     * @param offset           Pagination offset.
     * @param resultsPerPage   Number of results that should be returned starting at the offset.
     * @return EntityList
     */
    public EntityList getEntityList(String fieldStr, String nameSubsequence, String tagKeywords,
                                    String filterType, String filterTags, String filterBy,
                                    String orderBy, String sortOrder, Integer offset,
                                    Integer resultsPerPage, final String doAsUser) {
        return getEntityList(fieldStr, nameSubsequence, tagKeywords, filterType, filterTags, filterBy,
                orderBy, sortOrder, offset, resultsPerPage, doAsUser, false);
    }


    public EntityList getEntityList(String fieldStr, String nameSubsequence, String tagKeywords,
                                    String filterType, String filterTags, String filterBy,
                                    String orderBy, String sortOrder, Integer offset,
                                    Integer resultsPerPage, final String doAsUser, boolean isReturnAll) {
        HashSet<String> fields = new HashSet<String>(Arrays.asList(fieldStr.toUpperCase().split(",")));
        Map<String, List<String>> filterByFieldsValues = getFilterByFieldsValues(filterBy);
        for (String key : filterByFieldsValues.keySet()) {
            if (!key.toUpperCase().equals("NAME") && !key.toUpperCase().equals("CLUSTER")) {
                fields.add(key.toUpperCase());
            }
        }
        try {
            // get filtered entities
            List<Entity> entities = getEntityList(
                    nameSubsequence, tagKeywords, filterType, filterTags, filterBy, doAsUser);

            // sort entities and pagination
            List<Entity> entitiesReturn = sortEntitiesPagination(
                    entities, orderBy, sortOrder, offset, resultsPerPage, isReturnAll);

            // add total number of results
            EntityList entityList = entitiesReturn.size() == 0
                    ? new EntityList(new Entity[]{}, 0)
                    : new EntityList(buildEntityElements(new HashSet<String>(fields), entitiesReturn), entities.size());
            return entityList;
        } catch (Exception e) {
            LOG.error("Failed to get entity list", e);
            throw FalconWebException.newAPIException(e);
        }
    }

    public List<Entity> getEntityList(String nameSubsequence, String tagKeywords,
                                      String filterType, String filterTags, String filterBy, final String doAsUser)
        throws FalconException, IOException {

        Map<String, List<String>> filterByFieldsValues = getFilterByFieldsValues(filterBy);
        validateEntityFilterByClause(filterByFieldsValues);
        if (StringUtils.isNotEmpty(filterTags)) {
            filterByFieldsValues.put(EntityList.EntityFilterByFields.TAGS.name(), Arrays.asList(filterTags));
        }

        // get filtered entities
        List<Entity> entities = new ArrayList<Entity>();
        if (StringUtils.isEmpty(filterType)) {
            // return entities of all types if no entity type specified
            for (EntityType entityType : EntityType.values()) {
                entities.addAll(getFilteredEntities(
                        entityType, nameSubsequence, tagKeywords, filterByFieldsValues, "", "", "", doAsUser));
            }
        } else {
            String[] types = filterType.split(",");
            for (String type : types) {
                EntityType entityType = EntityType.getEnum(type);
                entities.addAll(getFilteredEntities(
                        entityType, nameSubsequence, tagKeywords, filterByFieldsValues, "", "", "", doAsUser));
            }
        }

        return entities;
    }

    protected List<Entity> sortEntitiesPagination(List<Entity> entities, String orderBy, String sortOrder,
                                                  Integer offset, Integer resultsPerPage) {
        return sortEntitiesPagination(entities, orderBy, sortOrder, offset, resultsPerPage, false);
    }

    protected List<Entity> sortEntitiesPagination(List<Entity> entities, String orderBy, String sortOrder,
                                                  Integer offset, Integer resultsPerPage, boolean isReturnAll) {
        // sort entities
        entities = sortEntities(entities, orderBy, sortOrder);

        // pagination
        int pageCount = getRequiredNumberOfResults(entities.size(), offset, resultsPerPage, isReturnAll);
        List<Entity> entitiesReturn = new ArrayList<Entity>();
        if (pageCount > 0) {
            entitiesReturn.addAll(entities.subList(offset, (offset + pageCount)));
        }

        return entitiesReturn;
    }

    protected Map<String, List<String>> validateEntityFilterByClause(Map<String, List<String>> filterByFieldsValues) {
        for (Map.Entry<String, List<String>> entry : filterByFieldsValues.entrySet()) {
            try {
                EntityList.EntityFilterByFields.valueOf(entry.getKey().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw FalconWebException.newAPIException("Invalid filter key: " + entry.getKey());
            }
        }
        return filterByFieldsValues;
    }

    protected Map<String, List<String>> validateEntityFilterByClause(String entityFilterByClause) {
        Map<String, List<String>> filterByFieldsValues = getFilterByFieldsValues(entityFilterByClause);
        return validateEntityFilterByClause(filterByFieldsValues);
    }

    protected List<Entity> getFilteredEntities(
            EntityType entityType, String nameSubsequence, String tagKeywords,
            Map<String, List<String>> filterByFieldsValues,
            String startDate, String endDate, String cluster, final String doAsUser)
        throws FalconException, IOException {
        Collection<String> entityNames = configStore.getEntities(entityType);
        if (entityNames.isEmpty()) {
            return Collections.emptyList();
        }

        List<Entity> entities = new ArrayList<Entity>();
        char[] subsequence = nameSubsequence.toLowerCase().toCharArray();
        List<String> tagKeywordsList;
        if (StringUtils.isEmpty(tagKeywords)) {
            tagKeywordsList = new ArrayList<>();
        } else {
            tagKeywordsList = getFilterByTags(Arrays.asList(tagKeywords.toLowerCase()));
        }
        for (String entityName : entityNames) {
            Entity entity;
            try {
                entity = configStore.get(entityType, entityName);
                if (entity == null) {
                    continue;
                }
            } catch (FalconException e1) {
                LOG.error("Unable to get list for entities for ({})", entityType.getEntityClass().getSimpleName(), e1);
                throw FalconWebException.newAPIException(e1);
            }

            if (SecurityUtil.isAuthorizationEnabled() && !isEntityAuthorized(entity)) {
                // the user who requested list query has no permission to access this entity. Skip this entity
                continue;
            }
            if (isFilteredByDatesAndCluster(entity, startDate, endDate, cluster)) {
                // this is for entity summary
                continue;
            }
            SecurityUtil.tryProxy(entity, doAsUser);

            // filter by fields
            if (isFilteredByFields(entity, filterByFieldsValues)) {
                continue;
            }

            // filter by subsequence of name
            if (subsequence.length > 0 && !matchesNameSubsequence(subsequence, entityName.toLowerCase())) {
                continue;
            }

            // filter by tag keywords
            if (!matchTagKeywords(tagKeywordsList, entity.getTags())) {
                continue;
            }

            entities.add(entity);
        }

        return entities;
    }

    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private boolean matchesNameSubsequence(char[] subsequence, String name) {
        int currentIndex = 0; // current index in pattern which is to be matched
        for (Character c : name.toCharArray()) {
            if (currentIndex < subsequence.length && c == subsequence[currentIndex]) {
                currentIndex++;
            }
            if (currentIndex == subsequence.length) {
                return true;
            }
        }
        return false;
    }

    private boolean matchTagKeywords(List<String> tagKeywords, String tags) {
        if (tagKeywords.isEmpty()) {
            return true;
        }

        if (StringUtils.isEmpty(tags)) {
            return false;
        }

        tags = tags.toLowerCase();
        for (String keyword : tagKeywords) {
            if (tags.indexOf(keyword) == -1) {
                return false;
            }
        }

        return true;
    }

    private boolean isFilteredByDatesAndCluster(Entity entity, String startDate, String endDate, String cluster)
        throws FalconException {
        if (StringUtils.isEmpty(cluster)) {
            return false; // no filtering necessary on cluster
        }
        Set<String> clusters = EntityUtil.getClustersDefined(entity);
        if (!clusters.contains(cluster)) {
            return true; // entity does not have this cluster
        }

        if (StringUtils.isNotEmpty(startDate)) {
            Date parsedDate = EntityUtil.parseDateUTC(startDate);
            if (parsedDate.after(EntityUtil.getEndTime(entity, cluster))) {
                return true;
            }
        }
        if (StringUtils.isNotEmpty(endDate)) {
            Date parseDate = EntityUtil.parseDateUTC(endDate);
            if (parseDate.before(EntityUtil.getStartTime(entity, cluster))) {
                return true;
            }
        }

        return false;
    }

    protected static Map<String, List<String>> getFilterByFieldsValues(String filterBy) {
        // Filter the results by specific field:value, eliminate empty values
        Map<String, List<String>> filterByFieldValues = new HashMap<String, List<String>>();
        if (StringUtils.isNotEmpty(filterBy)) {
            String[] fieldValueArray = filterBy.split(",");
            for (String fieldValue : fieldValueArray) {
                String[] splits = fieldValue.split(":", 2);
                String filterByField = splits[0];
                if (splits.length == 2 && !splits[1].equals("")) {
                    List<String> currentValue = filterByFieldValues.get(filterByField);
                    if (currentValue == null) {
                        currentValue = new ArrayList<String>();
                        filterByFieldValues.put(filterByField, currentValue);
                    }
                    currentValue.add(splits[1]);
                }
            }
        }

        return filterByFieldValues;
    }

    private static List<String> getFilterByTags(List<String> filterTags) {
        ArrayList<String> filterTagsList = new ArrayList<String>();
        if (filterTags!= null && !filterTags.isEmpty()) {
            for (String filterTag: filterTags) {
                String[] splits = filterTag.split(",");
                for (String tag : splits) {
                    filterTagsList.add(tag.trim());
                }
            }

        }
        return filterTagsList;
    }

    protected String getStatusString(Entity entity) {
        String statusString;
        try {
            statusString = getStatus(entity, entity.getEntityType()).first.name();
        } catch (Throwable throwable) {
            // Unable to fetch statusString, setting it to unknown for backwards compatibility
            statusString = "UNKNOWN";
        }
        return statusString;
    }

    protected boolean isEntityAuthorized(Entity entity) {
        try {
            SecurityUtil.getAuthorizationProvider().authorizeEntity(entity.getName(),
                    entity.getEntityType().toString(), entity.getACL(),
                    "list", CurrentUser.getAuthenticatedUGI());
        } catch (Exception e) {
            LOG.info("Authorization failed for entity=" + entity.getName()
                + " for user=" + CurrentUser.getUser(), e);
            return false;
        }

        return true;
    }

    private boolean isFilteredByTags(List<String> filterTagsList, List<String> tags) {
        if (filterTagsList.isEmpty()) {
            return false;
        } else if (tags.isEmpty()) {
            return true;
        }

        for (String tag : filterTagsList) {
            if (!tags.contains(tag)) {
                return true;
            }
        }

        return false;
    }

    private boolean isFilteredByPipelines(List<String> filterPipelinesList, List<String> pipelines) {
        if (filterPipelinesList.isEmpty()) {
            return false;
        } else if (pipelines.isEmpty()) {
            return true;
        }

        for (String pipeline : filterPipelinesList) {
            if (pipelines.contains(pipeline)) {
                return false;
            }
        }
        return true;
    }

    private boolean isFilteredByClusters(List<String> filterClustersList, Set<String> clusters) {
        if (filterClustersList.isEmpty()) {
            return false;
        } else if (clusters.isEmpty()) {
            return true;
        }

        for (String cluster : filterClustersList) {
            if (clusters.contains(cluster)) {
                return false;
            }
        }
        return true;
    }

    private boolean isFilteredByFields(Entity entity, Map<String, List<String>> filterKeyVals) {
        if (filterKeyVals.isEmpty()) {
            return false;
        }

        for (Map.Entry<String, List<String>> pair : filterKeyVals.entrySet()) {
            EntityList.EntityFilterByFields filter =
                    EntityList.EntityFilterByFields.valueOf(pair.getKey().toUpperCase());
            if (isEntityFiltered(entity, filter, pair)) {
                return true;
            }
        }

        return false;
    }

    private boolean isEntityFiltered(Entity entity, EntityList.EntityFilterByFields filter,
                                     Map.Entry<String, List<String>> pair) {
        switch (filter) {
        case TYPE:
            return !containsIgnoreCase(pair.getValue(), entity.getEntityType().toString());

        case NAME:
            return !containsIgnoreCase(pair.getValue(), entity.getName());

        case STATUS:
            return !containsIgnoreCase(pair.getValue(), getStatusString(entity));

        case PIPELINES:
            if (!entity.getEntityType().equals(EntityType.PROCESS)) {
                throw FalconWebException.newAPIException("Invalid filterBy key for non"
                    + " process entities " + pair.getKey());

            }
            return isFilteredByPipelines(pair.getValue(), EntityUtil.getPipelines(entity));

        case CLUSTER:
            return isFilteredByClusters(pair.getValue(), EntityUtil.getClustersDefined(entity));

        case TAGS:
            return isFilteredByTags(getFilterByTags(pair.getValue()), EntityUtil.getTags(entity));

        default:
            return false;
        }
    }

    private List<Entity> sortEntities(List<Entity> entities, String orderBy, String sortOrder) {
        // Sort the ArrayList using orderBy param
        if (!entities.isEmpty() && StringUtils.isNotEmpty(orderBy)) {
            EntityList.EntityFieldList orderByField = EntityList.EntityFieldList.valueOf(orderBy.toUpperCase());
            final String order = getValidSortOrder(sortOrder, orderBy);
            switch (orderByField) {

            case NAME:
                Collections.sort(entities, new Comparator<Entity>() {
                    @Override
                    public int compare(Entity e1, Entity e2) {
                        return (order.equalsIgnoreCase("asc")) ? e1.getName().compareTo(e2.getName())
                                : e2.getName().compareTo(e1.getName());
                    }
                });
                break;

            default:
                break;
            }
        } // else no sort

        return entities;
    }

    protected String getValidSortOrder(String sortOrder, String orderBy) {
        if (StringUtils.isEmpty(sortOrder)) {
            return (orderBy.equalsIgnoreCase("starttime")
                    || orderBy.equalsIgnoreCase("endtime")) ? "desc" : "asc";
        }

        if (sortOrder.equalsIgnoreCase("asc") || sortOrder.equalsIgnoreCase("desc")) {
            return sortOrder;
        }

        String err = "Value for param sortOrder should be \"asc\" or \"desc\". It is  : " + sortOrder;
        LOG.error(err);
        throw FalconWebException.newAPIException(err);
    }

    protected int getRequiredNumberOfResults(int arraySize, int offset, int numresults) {
        return getRequiredNumberOfResults(arraySize, offset, numresults, false);
    }

    protected int getRequiredNumberOfResults(int arraySize, int offset, int numresults, boolean isReturnAll) {
        /* Get a subset of elements based on offset and count. When returning subset of elements,
              elements[offset] is included. Size 10, offset 10, return empty list.
              Size 10, offset 5, count 3, return elements[5,6,7].
              Size 10, offset 5, count >= 5, return elements[5,6,7,8,9]
              return elements starting from elements[offset] until the end OR offset+numResults*/

        if (!isReturnAll && numresults < 1) {
            LOG.error("Value for param numResults should be > than 0  : {}", numresults);
            throw FalconWebException.newAPIException("Value for param numResults should be > than 0  : " + numresults);
        }

        if (offset < 0) { offset = 0; }

        if (offset >= arraySize || arraySize == 0) {
            // No elements to return
            return 0;
        }

        int retLen = arraySize - offset;
        if (!isReturnAll && retLen > numresults) {
            retLen = numresults;
        }
        return retLen;
    }

    protected EntityElement[] buildEntityElements(HashSet<String> fields, List<Entity> entities) {
        EntityElement[] elements = new EntityElement[entities.size()];
        int elementIndex = 0;
        for (Entity entity : entities) {
            elements[elementIndex++] = getEntityElement(entity, fields);
        }
        return elements;
    }

    protected EntityElement getEntityElement(Entity entity, HashSet<String> fields) {
        EntityElement elem = new EntityElement();
        elem.type = entity.getEntityType().toString();
        elem.name = entity.getName();
        if (fields.contains(EntityList.EntityFieldList.STATUS.name())) {
            elem.status = getStatusString(entity);
        }
        if (fields.contains(EntityList.EntityFieldList.PIPELINES.name())) {
            elem.pipeline = EntityUtil.getPipelines(entity);
        }
        if (fields.contains(EntityList.EntityFieldList.TAGS.name())) {
            elem.tag = EntityUtil.getTags(entity);
        }
        if (fields.contains(EntityList.EntityFieldList.CLUSTERS.name())) {
            elem.cluster = new ArrayList<String>(EntityUtil.getClustersDefined(entity));
        }
        return elem;
    }

    /**
     * Returns the entity definition as an XML based on name.
     *
     * @param type       entity type
     * @param entityName entity name
     * @return String
     */
    public String getEntityDefinition(String type, String entityName) {
        try {
            EntityType entityType = EntityType.getEnum(type);
            Entity entity = configStore.get(entityType, entityName);
            if (entity == null) {
                throw new NoSuchElementException(entityName + " (" + type + ") not found");
            }
            return entity.toString();
        } catch (Throwable e) {
            LOG.error("Unable to get entity definition from config store for ({}): {}", type, entityName, e);
            throw FalconWebException.newAPIException(e);

        }
    }


    /**
     * Given the location of data, returns the feed.
     * @param type type of the entity, is valid only for feeds.
     * @param instancePath location of the data
     * @return Feed Name, type of the data and cluster name.
     */
    public FeedLookupResult reverseLookup(String type, String instancePath) {
        try {
            EntityType entityType = EntityType.getEnum(type);
            if (entityType != EntityType.FEED) {
                LOG.error("Reverse Lookup is not supported for entitytype: {}", type);
                throw new IllegalArgumentException("Reverse lookup is not supported for " + type);
            }

            instancePath = StringUtils.trim(instancePath);
            String instancePathWithoutSlash =
                    instancePath.endsWith("/") ? StringUtils.removeEnd(instancePath, "/") : instancePath;
            // treat strings with and without trailing slash as same for purpose of searching e.g.
            // /data/cas and /data/cas/ should be treated as same.
            String instancePathWithSlash = instancePathWithoutSlash + "/";
            FeedLocationStore store = FeedLocationStore.get();
            Collection<FeedLookupResult.FeedProperties> feeds = new ArrayList<>();
            Collection<FeedLookupResult.FeedProperties> res = store.reverseLookup(instancePathWithoutSlash);
            if (res != null) {
                feeds.addAll(res);
            }
            res = store.reverseLookup(instancePathWithSlash);
            if (res != null) {
                feeds.addAll(res);
            }
            FeedLookupResult result = new FeedLookupResult(APIResult.Status.SUCCEEDED, "SUCCESS");
            FeedLookupResult.FeedProperties[] props = feeds.toArray(new FeedLookupResult.FeedProperties[0]);
            result.setElements(props);
            return result;

        } catch (IllegalArgumentException e) {
            throw FalconWebException.newAPIException(e);
        } catch (Throwable throwable) {
            LOG.error("reverse look up failed", throwable);
            throw FalconWebException.newAPIException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    protected AbstractWorkflowEngine getWorkflowEngine(Entity entity) throws FalconException {
        return WorkflowEngineFactory.getWorkflowEngine(entity);
    }

    protected <T extends APIResult> T consolidateResult(Map<String, T> results, Class<T> clazz) {
        if (results == null || results.isEmpty()) {
            return null;
        }

        StringBuilder message = new StringBuilder();
        StringBuilder requestIds = new StringBuilder();
        List instances = new ArrayList();
        int statusCount = 0;
        for (Map.Entry<String, T> entry : results.entrySet()) {
            String colo = entry.getKey();
            T result = results.get(colo);
            message.append(colo).append('/').append(result.getMessage()).append('\n');
            requestIds.append(colo).append('/').append(result.getRequestId()).append('\n');
            statusCount += result.getStatus().ordinal();

            if (result.getCollection() == null) {
                continue;
            }
            Collections.addAll(instances, result.getCollection());
        }
        Object[] arrInstances = instances.toArray();
        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED
                : ((statusCount == results.size() * 2) ? APIResult.Status.FAILED : APIResult.Status.PARTIAL);
        try {
            Constructor<T> constructor = clazz.getConstructor(Status.class, String.class);
            T result = constructor.newInstance(status, message.toString());
            result.setCollection(arrInstances);
            result.setRequestId(requestIds.toString());
            return result;
        } catch (Exception e) {
            throw new FalconRuntimException("Unable to consolidate result.", e);
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

    private void verifySuperUser() throws FalconException, IOException {
        final UserGroupInformation authenticatedUGI = CurrentUser.getAuthenticatedUGI();
        DefaultAuthorizationProvider authorizationProvider = new DefaultAuthorizationProvider();
        if (!authorizationProvider.isSuperUser(authenticatedUGI)) {
            throw new FalconException("Permission denied : "
                    + "Cluster entity update can only be performed by superuser.");
        }
    }
}
