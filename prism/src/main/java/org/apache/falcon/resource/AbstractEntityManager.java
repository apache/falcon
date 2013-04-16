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
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.store.EntityAlreadyExistsException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityGraph;
import org.apache.falcon.entity.v0.EntityIntegrityChecker;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.APIResult.Status;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public abstract class AbstractEntityManager {
    private static final Logger LOG = Logger.getLogger(AbstractEntityManager.class);
    private static final Logger AUDIT = Logger.getLogger("AUDIT");
    protected static final int XML_DEBUG_LEN = 10 * 1024;

    private AbstractWorkflowEngine workflowEngine;
    protected ConfigurationStore configStore = ConfigurationStore.get();

    public AbstractEntityManager() {
        try {
            workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
        } catch (FalconException e) {
            throw new FalconRuntimException(e);
        }
    }

    protected void checkColo(String colo) throws FalconWebException {
        if (!DeploymentUtil.getCurrentColo().equals(colo)) {
            throw FalconWebException.newException(
                    "Current colo (" + DeploymentUtil.getCurrentColo() + ") is not " + colo,
                    Response.Status.BAD_REQUEST);
        }
    }

    protected Set<String> getAllColos() {
        if (DeploymentUtil.isEmbeddedMode()) {
            return DeploymentUtil.getDefaultColos();
        }
        String[] colos = RuntimeProperties.get().getProperty("all.colos", DeploymentUtil.getDefaultColo()).split(",");
        return new HashSet<String>(Arrays.asList(colos));
    }

    protected Set<String> getColosFromExpression(String coloExpr, String type, String entity) {
        Set<String> colos;
        if (coloExpr == null || coloExpr.equals("*") || coloExpr.isEmpty()) {
            colos = getApplicableColos(type, entity);
        } else {
            colos = new HashSet<String>(Arrays.asList(coloExpr.split(",")));
        }
        return colos;
    }

    protected Set<String> getApplicableColos(String type, String name) throws FalconWebException {
        try {
            if (DeploymentUtil.isEmbeddedMode()) {
                return DeploymentUtil.getDefaultColos();
            }

            if (EntityType.valueOf(type.toUpperCase()) == EntityType.CLUSTER) {
                return getAllColos();
            }

            return getApplicableColos(type, EntityUtil.getEntity(type, name));
        } catch (FalconException e) {
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    protected Set<String> getApplicableColos(String type, Entity entity) throws FalconWebException {
        try {
            if (DeploymentUtil.isEmbeddedMode()) {
                return DeploymentUtil.getDefaultColos();
            }

            if (EntityType.valueOf(type.toUpperCase()) == EntityType.CLUSTER) {
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
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
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
            audit(request, "STREAMED_DATA", type, "SUBMIT");
            Entity entity = submitInternal(request, type);
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + type + ") " + entity.getName());
        } catch (Throwable e) {
            LOG.error("Unable to persist entity object", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Post an entity XML with entity type. Validates the XML which can be
     * Process, Feed or Dataendpoint
     *
     * @param type
     * @return APIResule -Succeeded or Failed
     */
    public APIResult validate(HttpServletRequest request, String type) {
        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            Entity entity = deserializeEntity(request, entityType);
            validate(entity);
            return new APIResult(APIResult.Status.SUCCEEDED,
                    "Validated successfully (" + entityType + ") " + entity.getName());
        } catch (Throwable e) {
            LOG.error("Validation failed for entity (" + type + ") ", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Deletes a scheduled entity, a deleted entity is removed completely from
     * execution pool.
     *
     * @param type
     * @param entity
     * @return APIResult
     */
    public APIResult delete(HttpServletRequest request, String type, String entity, String colo) {

        checkColo(colo);
        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            audit(request, entity, type, "DELETE");
            String removedFromEngine = "";
            try {
                Entity entityObj = EntityUtil.getEntity(type, entity);

                canRemove(entityObj);
                if (entityType.isSchedulable() && !DeploymentUtil.isPrism()) {
                    getWorkflowEngine().delete(entityObj);
                    removedFromEngine = "(KILLED in ENGINE)";
                }

                configStore.remove(entityType, entity);
            } catch (EntityNotRegisteredException e) { // already deleted
                return new APIResult(APIResult.Status.SUCCEEDED,
                        entity + "(" + type + ") doesn't exist. Nothing to do");
            }

            return new APIResult(APIResult.Status.SUCCEEDED,
                    entity + "(" + type + ") removed successfully " + removedFromEngine);
        } catch (Throwable e) {
            LOG.error("Unable to reach workflow engine for deletion or " + "deletion failed", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    // Parallel update can get very clumsy if two feeds are updated which
    // are referred by a single process. Sequencing them.
    public synchronized APIResult update(HttpServletRequest request, String type, String entityName, String colo) {
        checkColo(colo);
        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            audit(request, entityName, type, "UPDATE");
            Entity oldEntity = EntityUtil.getEntity(type, entityName);
            Entity newEntity = deserializeEntity(request, entityType);
            validate(newEntity);

            validateUpdate(oldEntity, newEntity);
            if (!EntityUtil.equals(oldEntity, newEntity)) {
                configStore.initiateUpdate(newEntity);
                //Update in workflow engine
                if (!DeploymentUtil.isPrism()) {
                    Set<String> oldClusters = EntityUtil.getClustersDefinedInColos(oldEntity);
                    Set<String> newClusters = EntityUtil.getClustersDefinedInColos(newEntity);
                    newClusters.retainAll(oldClusters); //common clusters for update
                    oldClusters.removeAll(newClusters); //deleted clusters

                    for (String cluster : newClusters) {
                        getWorkflowEngine().update(oldEntity, newEntity, cluster);
                    }
                    for (String cluster : oldClusters) {
                        getWorkflowEngine().delete(oldEntity, cluster);
                    }
                }

                configStore.update(entityType, newEntity);
            }

            return new APIResult(APIResult.Status.SUCCEEDED, entityName + " updated successfully");
        } catch (Throwable e) {
            LOG.error("Updation failed", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        } finally {
            ConfigurationStore.get().cleanupUpdateInit();
        }
    }

    private void validateUpdate(Entity oldEntity, Entity newEntity) throws FalconException {
        if (oldEntity.getEntityType() != newEntity.getEntityType() || !oldEntity.equals(newEntity)) {
            throw new FalconException(
                    oldEntity.toShortString() + " can't be updated with " + newEntity.toShortString());
        }

        if (oldEntity.getEntityType() == EntityType.CLUSTER) {
            throw new FalconException("Update not supported for clusters");
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

    private void canRemove(Entity entity) throws FalconException {
        Pair<String, EntityType>[] referencedBy = EntityIntegrityChecker.referencedBy(entity);
        if (referencedBy != null && referencedBy.length > 0) {
            StringBuffer messages = new StringBuffer();
            for (Pair<String, EntityType> ref : referencedBy) {
                messages.append(ref).append("\n");
            }
            throw new FalconException(
                    entity.getName() + "(" + entity.getEntityType() + ") cant " + "be removed as it is referred by "
                            + messages);
        }
    }

    protected synchronized Entity submitInternal(HttpServletRequest request, String type)
            throws IOException, FalconException {

        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        Entity entity = deserializeEntity(request, entityType);

        Entity existingEntity = configStore.get(entityType, entity.getName());
        if (existingEntity != null) {
            if (EntityUtil.equals(existingEntity, entity)) {
                return existingEntity;
            }

            throw new EntityAlreadyExistsException(
                    entity.toShortString() + " already registered with configuration store. "
                            + "Can't be submitted again. Try removing before submitting.");
        }

        validate(entity);
        configStore.publish(entityType, entity);
        LOG.info("Submit successful: (" + type + ")" + entity.getName());
        return entity;
    }

    protected Entity deserializeEntity(HttpServletRequest request, EntityType entityType)
            throws IOException, FalconException {

        EntityParser<?> entityParser = EntityParserFactory.getParser(entityType);
        InputStream xmlStream = request.getInputStream();
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
                    LOG.debug("XML DUMP for (" + entityType + "): " + xmlData, e);
                } catch (IOException ignore) {
                }
            }
            throw e;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void validate(Entity entity) throws FalconException {
        EntityParser entityParser = EntityParserFactory.getParser(entity.getEntityType());
        entityParser.validate(entity);
    }

    private String getAsString(InputStream xmlStream) throws IOException {
        byte[] data = new byte[XML_DEBUG_LEN];
        IOUtils.readFully(xmlStream, data, 0, XML_DEBUG_LEN);
        return new String(data);
    }

    protected void audit(HttpServletRequest request, String entity, String type, String action) {
        if (request == null) {
            return; // this must be internal call from Falcon
        }
        AUDIT.info("Performed " + action + " on " + entity + "(" + type + ") :: " + request.getRemoteHost() + "/"
                + CurrentUser.getUser());
    }

    private enum EntityStatus {
        SUBMITTED, SUSPENDED, RUNNING
    }

    /**
     * Returns the status of requested entity.
     *
     * @param type
     * @param entity
     * @return String
     */
    public APIResult getStatus(String type, String entity, String colo) {

        checkColo(colo);
        Entity entityObj = null;
        try {
            entityObj = EntityUtil.getEntity(type, entity);
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            String status;

            if (entityType.isSchedulable()) {
                if (workflowEngine.isActive(entityObj)) {
                    if (workflowEngine.isSuspended(entityObj)) {
                        status = EntityStatus.SUSPENDED.name();
                    } else {
                        status = EntityStatus.RUNNING.name();
                    }
                } else {
                    status = EntityStatus.SUBMITTED.name();
                }
            } else {
                status = EntityStatus.SUBMITTED.name();
            }
            return new APIResult(Status.SUCCEEDED, status);
        } catch (FalconWebException e) {
            throw e;
        } catch (Exception e) {

            LOG.error("Unable to get status for entity " + entity + "(" + type + ")", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Returns dependencies.
     *
     * @param type
     * @param entity
     * @return EntityList
     */
    public EntityList getDependencies(String type, String entity) {

        try {
            Entity entityObj = EntityUtil.getEntity(type, entity);
            Set<Entity> dependents = EntityGraph.get().getDependents(entityObj);
            Entity[] entities = dependents.toArray(new Entity[dependents.size()]);
            return new EntityList(entities == null ? new Entity[]{} : entities);
        } catch (Exception e) {
            LOG.error("Unable to get dependencies for entity " + entity + "(" + type + ")", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Returns the list of entities registered of a given type.
     *
     * @param type
     * @return String
     */
    public EntityList getDependencies(String type) {
        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            Collection<String> entityNames = configStore.getEntities(entityType);
            if (entityNames == null || entityNames.equals("")) {
                return new EntityList(new Entity[]{});
            }
            Entity[] entities = new Entity[entityNames.size()];
            int index = 0;
            for (String entityName : entityNames) {
                entities[index++] = configStore.get(entityType, entityName);
            }
            return new EntityList(entities);
        } catch (Exception e) {
            LOG.error("Unable to get list for entities for (" + type + ")", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Returns the entity definition as an XML based on name
     *
     * @param type
     * @param entityName
     * @return String
     */
    public String getEntityDefinition(String type, String entityName) {
        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            Entity entity = configStore.get(entityType, entityName);
            if (entity == null) {
                throw new NoSuchElementException(entityName + " (" + type + ") not found");
            }
            return entity.toString();
        } catch (Throwable e) {
            LOG.error("Unable to get entity definition from config " + "store for (" + type + ") " + entityName, e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);

        }
    }

    protected AbstractWorkflowEngine getWorkflowEngine() {
        return this.workflowEngine;
    }

}
