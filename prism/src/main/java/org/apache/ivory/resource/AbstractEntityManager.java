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

package org.apache.ivory.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.Pair;
import org.apache.ivory.entity.EntityNotRegisteredException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.parser.EntityParser;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.parser.ValidationException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.EntityAlreadyExistsException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityIntegrityChecker;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.resource.APIResult.Status;
import org.apache.ivory.resource.InstancesResult.Instance;
import org.apache.ivory.security.CurrentUser;
import org.apache.ivory.service.IvoryService;
import org.apache.ivory.util.DeploymentUtil;
import org.apache.ivory.util.RuntimeProperties;
import org.apache.ivory.workflow.WorkflowEngineFactory;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

public abstract class AbstractEntityManager implements IvoryService {
    private static final Logger LOG = Logger.getLogger(AbstractEntityManager.class);
    private static final Logger AUDIT = Logger.getLogger("AUDIT");
    protected static final int XML_DEBUG_LEN = 10 * 1024;

    private WorkflowEngine workflowEngine;
    protected ConfigurationStore configStore = ConfigurationStore.get();

    public AbstractEntityManager() {
        try {
            workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
        } catch (IvoryException e) {
            throw new IvoryRuntimException(e);
        }
    }

    @Override
    public void init() throws IvoryException {
    }

    @Override
    public void destroy() throws IvoryException {
    }

    protected void checkColo(String colo) throws IvoryWebException {
        if (!DeploymentUtil.getCurrentColo().equals(colo)) {
            throw IvoryWebException.newException("Current colo (" + DeploymentUtil.getCurrentColo() + ") is not " + colo,
                    Response.Status.BAD_REQUEST);
        }
    }

    protected APIResult consolidateResult(Map<String, APIResult> results) {
        if (results == null || results.size() == 0)
            return null;

        StringBuilder buffer = new StringBuilder();
        StringBuilder requestIds = new StringBuilder();
        int statusCount = 0;
        for (Entry<String, APIResult> entry : results.entrySet()) {
            String colo = entry.getKey();
            APIResult result = entry.getValue();
            buffer.append(colo).append('/').append(result.getMessage()).append('\n');
            requestIds.append(colo).append('/').append(result.getRequestId()).append('\n');
            statusCount += result.getStatus().ordinal();
        }

        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED
                : ((statusCount == results.size() * 2) ? APIResult.Status.FAILED : APIResult.Status.PARTIAL);
        APIResult result = new APIResult(status, buffer.toString());
        result.setRequestId(requestIds.toString());
        return result;
    }

    protected InstancesResult consolidateInstanceResult(APIResult[] results, String[] colos) {
        if (results == null || results.length == 0)
            return null;

        StringBuilder message = new StringBuilder();
        StringBuilder requestIds = new StringBuilder();
        List<Instance> instances = new ArrayList<Instance>();
        int statusCount = 0;
        for (int index = 0; index < results.length; index++) {
            APIResult result = results[index];
            message.append(colos[index]).append('/').append(result.getMessage()).append('\n');
            requestIds.append(colos[index]).append('/').append(results[index].getRequestId()).append('\n');

            if (!(result instanceof InstancesResult)) continue;
            InstancesResult instancesResult = (InstancesResult) result;
            if (instancesResult.getInstances() == null) continue;

            for (Instance instance : instancesResult.getInstances()) {
                Instance instClone = new Instance(instance.cluster,
                        colos[index] + "/" + instance.getInstance(), instance.getStatus());
                instances.add(new Instance(instClone, instance.logFile, instance.actions));
            }
            statusCount += results[index].getStatus().ordinal();
        }
        Instance[] arrInstances = new Instance[instances.size()];
        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED
                : ((statusCount == results.length * 2) ? APIResult.Status.FAILED : APIResult.Status.PARTIAL);
        InstancesResult result = new InstancesResult(status, message.toString(), instances.toArray(arrInstances));
        result.setRequestId(requestIds.toString());
        return result;
    }

    protected String[] getAllColos() {
        if (DeploymentUtil.isEmbeddedMode())
            return DeploymentUtil.getDefaultColos();
        return RuntimeProperties.get().getProperty("all.colos", DeploymentUtil.getDefaultColo()).split(",");
    }

    protected String[] getColosFromExpression(String coloExpr, String type, String entity) {
        String[] colos;
        if (coloExpr == null || coloExpr.equals("*") || coloExpr.isEmpty()) {
            colos = getApplicableColos(type, entity);
        } else {
            colos = coloExpr.split(",");
        }
        return colos;
    }

    protected String[] getApplicableColos(String type, String name) throws IvoryWebException {
        try {
            if (DeploymentUtil.isEmbeddedMode())
                return DeploymentUtil.getDefaultColos();

            if (EntityType.valueOf(type.toUpperCase()) == EntityType.CLUSTER)
                return getAllColos();

            Entity entity = EntityUtil.getEntity(type, name);
            String[] clusters = EntityUtil.getClustersDefined(entity);
            Set<String> colos = new HashSet<String>();
            for (String cluster : clusters) {
                Cluster clusterEntity = (Cluster) EntityUtil.getEntity(EntityType.CLUSTER, cluster);
                colos.add(clusterEntity.getColo());
            }
            return colos.toArray(new String[colos.size()]);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system
     * 
     * Entity name acts as the key and an entity once added, can't be added
     * again unless deleted.
     * 
     * @param request
     *            - Servlet Request
     * @param type
     *            - entity type - feed, process or data end point
     * @param colo
     *            - applicable colo
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
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
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
            return new APIResult(APIResult.Status.SUCCEEDED, "Validated successfully (" + entityType + ") " + entity.getName());
        } catch (Throwable e) {
            LOG.error("Validation failed for entity (" + type + ") ", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
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
                if (entityType.isSchedulable() && getWorkflowEngine().isActive(entityObj)) {
                    getWorkflowEngine().delete(entityObj);
                    removedFromEngine = "(KILLED in ENGINE)";
                }

                configStore.remove(entityType, entity);
            } catch (EntityNotRegisteredException e) { // already deleted
            }

            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + type + ") removed successfully " + removedFromEngine);
        } catch (Throwable e) {
            LOG.error("Unable to reach workflow engine for deletion or " + "deletion failed", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
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
            if (!oldEntity.deepEquals(newEntity)) {
                configStore.initiateUpdate(newEntity);
                getWorkflowEngine().update(oldEntity, newEntity);
                configStore.update(entityType, newEntity);
            }

            return new APIResult(APIResult.Status.SUCCEEDED, entityName + " updated successfully");
        } catch (Throwable e) {
            LOG.error("Updation failed", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        } finally {
            ConfigurationStore.get().cleanupUpdateInit();
        }
    }

    private void validateUpdate(Entity oldEntity, Entity newEntity) throws IvoryException {
        if (oldEntity.getEntityType() != newEntity.getEntityType())
            throw new IvoryException(oldEntity.toShortString() + " can't be updated with " + newEntity.toShortString());

        if (oldEntity.getEntityType() == EntityType.CLUSTER)
            throw new IvoryException("Update not supported for clusters");

        String[] props = oldEntity.getEntityType().getImmutableProperties();
        for (String prop : props) {
            Object oldProp, newProp;
            try {
                oldProp = PropertyUtils.getProperty(oldEntity, prop);
                newProp = PropertyUtils.getProperty(newEntity, prop);
            } catch (Exception e) {
                throw new IvoryException(e);
            }
            if (!ObjectUtils.equals(oldProp, newProp))
                throw new ValidationException(oldEntity.toShortString() + ": " + prop + " can't be changed");
        }
    }

    private void canRemove(Entity entity) throws IvoryException {
        Pair<String, EntityType>[] referencedBy = EntityIntegrityChecker.referencedBy(entity);
        if (referencedBy != null && referencedBy.length > 0) {
            StringBuffer messages = new StringBuffer();
            for (Pair<String, EntityType> ref : referencedBy) {
                messages.append(ref).append("\n");
            }
            throw new IvoryException(entity.getName() + "(" + entity.getEntityType() + ") cant " + "be removed as it is referred by "
                    + messages);
        }
    }

    protected synchronized Entity submitInternal(HttpServletRequest request, String type) throws IOException, IvoryException {

        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        Entity entity = deserializeEntity(request, entityType);

        ConfigurationStore configStore = ConfigurationStore.get();
        Entity existingEntity = configStore.get(entityType, entity.getName());
        if (existingEntity != null) {
            if (existingEntity.deepEquals(entity))
                return existingEntity;

            throw new EntityAlreadyExistsException(entity.toShortString() + " already registered with configuration store. "
                    + "Can't be submitted again. Try removing before submitting.");
        }

        validate(entity);
        configStore.publish(entityType, entity);
        LOG.info("Submit successful: (" + type + ")" + entity.getName());
        return entity;
    }

    protected Entity deserializeEntity(HttpServletRequest request, EntityType entityType) throws IOException, IvoryException {

        EntityParser<?> entityParser = EntityParserFactory.getParser(entityType);
        InputStream xmlStream = request.getInputStream();
        if (xmlStream.markSupported()) {
            xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
        }
        try {
            return entityParser.parse(xmlStream);
        } catch (IvoryException e) {
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void validate(Entity entity) throws IvoryException {
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
            return; // this must be internal call from Ivory
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
        } catch (IvoryWebException e) {
            throw e;
        } catch (Exception e) {

            LOG.error("Unable to get status for entity " + entity + "(" + type + ")", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
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
            return new EntityList(entities == null ? new Entity[] {} : entities);
        } catch (Exception e) {
            LOG.error("Unable to get dependencies for entity " + entity + "(" + type + ")", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
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
                return new EntityList(new Entity[] {});
            }
            Entity[] entities = new Entity[entityNames.size()];
            int index = 0;
            for (String entityName : entityNames) {
                entities[index++] = configStore.get(entityType, entityName);
            }
            return new EntityList(entities);
        } catch (Exception e) {
            LOG.error("Unable to get list for entities for (" + type + ")", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
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
            ConfigurationStore configStore = ConfigurationStore.get();
            Entity entity = configStore.get(entityType, entityName);
            if (entity == null) {
                throw new NoSuchElementException(entityName + " (" + type + ") not found");
            }
            return entity.toString();
        } catch (Throwable e) {
            LOG.error("Unable to get entity definition from config " + "store for (" + type + ") " + entityName, e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);

        }
    }

    protected WorkflowEngine getWorkflowEngine() {
        return this.workflowEngine;
    }
}
