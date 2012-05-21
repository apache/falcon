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

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.Pair;
import org.apache.ivory.entity.parser.EntityParser;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.parser.ValidationException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.EntityAlreadyExistsException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityIntegrityChecker;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.resource.ProcessInstancesResult.ProcessInstance;
import org.apache.ivory.security.CurrentUser;
import org.apache.ivory.service.IvoryService;
import org.apache.ivory.transaction.TransactionManager;
import org.apache.ivory.util.DeploymentProperties;
import org.apache.ivory.util.RuntimeProperties;
import org.apache.ivory.util.StartupProperties;
import org.apache.ivory.workflow.WorkflowEngineFactory;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public abstract class AbstractEntityManager implements IvoryService {
    private static final Logger LOG = Logger.getLogger(AbstractEntityManager.class);
    private static final Logger AUDIT = Logger.getLogger("AUDIT");
    protected static final int XML_DEBUG_LEN = 10 * 1024;
    protected static final String DEFAULT_COLO = "default";
    protected static final String INTEGRATED = "integrated";
    protected static final String DEPLOY_MODE = "deploy.mode";
    private static final String[] DEFAULT_ALL_COLOS = new String[] {DEFAULT_COLO};

    protected final String currentColo;
    protected final boolean integratedMode;

    private WorkflowEngine workflowEngine;
    protected ConfigurationStore configStore = ConfigurationStore.get();

    public AbstractEntityManager() {
        try {
            workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
            integratedMode = DeploymentProperties.get().
                    getProperty(DEPLOY_MODE, INTEGRATED).equals(INTEGRATED);
            if (integratedMode) {
                currentColo = DEFAULT_COLO;
            } else {
                currentColo = StartupProperties.get().
                    getProperty("current.colo", DEFAULT_COLO);
            }
            LOG.info("Running in integrated mode? " + integratedMode);
            LOG.info("Current colo: " + currentColo);
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
        if (!currentColo.equals(colo)) {
            throw IvoryWebException.newException("Current colo (" +
                    currentColo + ") is not " + colo, Response.Status.BAD_REQUEST);
        }
    }

    protected APIResult consolidatedResult(APIResult[] results, String[] colos) {
        if (results == null || results.length == 0) return null;

        StringBuilder buffer = new StringBuilder();
        StringBuilder requestIds = new StringBuilder();
        int statusCount = 0;
        for (int index = 0; index < results.length; index++) {
            buffer.append(colos[index]).append('/')
                    .append(results[index].getMessage())
                    .append(',').append(", REQ. ID is ")
                    .append(results[index].getRequestId()).append('\n');
            statusCount += results[index].getStatus().ordinal();
        }
        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED :
                ((statusCount == results.length * 2) ? APIResult.Status.FAILED :
                        APIResult.Status.PARTIAL);
        return new APIResult(status, buffer.toString(), requestIds.toString());
    }

    protected ProcessInstancesResult consolidatedInstanceResult(APIResult[] results,
                                                        String[] colos) {
        if (results == null || results.length == 0) return null;

        StringBuilder message = new StringBuilder("CONSOLIDATED");
        List<ProcessInstance> instances = new ArrayList<ProcessInstance>();
        int statusCount = 0;
        for (int index = 0; index < results.length; index++) {
            APIResult result = results[index];
            message.append('\n').append(colos[index]).append(": ").append(result.getMessage());
            if (!(result instanceof ProcessInstancesResult)) continue;
            for (ProcessInstance instance: ((ProcessInstancesResult) result).getInstances()) {
                ProcessInstance instClone = new ProcessInstance(colos[index] +
                        "/" + instance.getInstance(), instance.getStatus());
                instances.add(new ProcessInstance(instClone,
                        instance.logFile, instance.actions));
            }
            statusCount += results[index].getStatus().ordinal();
        }
        ProcessInstance[] arrInstances = new ProcessInstance[instances.size()];
        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED :
                ((statusCount == results.length * 2) ? APIResult.Status.FAILED :
                        APIResult.Status.PARTIAL);
        return new ProcessInstancesResult(status, message.toString(),
                instances.toArray(arrInstances));
    }

    protected String[] getAllColos() {
        if (integratedMode) {
            return DEFAULT_ALL_COLOS;
        } else {
            return RuntimeProperties.get().
                    getProperty("all.colos", DEFAULT_COLO).split(",");
        }
    }

    protected String[] getColosToApply(String colo) {
        String[] colos;
        if ( colo == null || colo.equals("*") || colo.isEmpty()) {
            colos = getAllColos();
        } else {
            colos = colo.split(",");
        }
        return colos;
    }

    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system
     * 
     * Entity name acts as the key and an entity once added, can't be added
     * again unless deleted.
     * 
     * @param request - Servlet Request
     * @param type - entity type
     *            - feed, process or data end point
     * @param colo - applicable colo
     * @return result of the operation
     */
    public APIResult submit(HttpServletRequest request, String type, String colo) {

        checkColo(colo);
        try {
            TransactionManager.startTransaction();
            audit(request, "STREAMED_DATA", type, "SUBMIT");
            Entity entity = submitInternal(request, type);
            APIResult result = new APIResult(APIResult.Status.SUCCEEDED,
                    "Submit successful (" + type + ") " + entity.getName(),
                    TransactionManager.getTransactionId());
            TransactionManager.commit();
            return result;
        } catch (Throwable e) {
            LOG.error("Unable to persist entity object", e);
            TransactionManager.rollback();
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
            return new APIResult(APIResult.Status.SUCCEEDED,
                    "Validated successfully (" + entityType + ") " + entity.getName());
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
    public APIResult delete(HttpServletRequest request, String type,
                            String entity, String colo) {

        checkColo(colo);
        try {
            TransactionManager.startTransaction();
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            audit(request, entity, type, "DELETE");
            String removedFromEngine = "";
            Entity entityObj = getEntityObject(entity, type);

            canRemove(entityObj);

            if (entityType.isSchedulable()) {
                if (getWorkflowEngine().isActive(entityObj)) {
                    getWorkflowEngine().delete(entityObj);
                    removedFromEngine = "(KILLED in ENGINE)";
                }
            }
            configStore.remove(entityType, entity);
            APIResult result = new APIResult(APIResult.Status.SUCCEEDED,
                    entity + "(" + type + ") removed successfully "
                    + removedFromEngine, TransactionManager.getTransactionId());
            TransactionManager.commit();
          
            return result;
        } catch (Throwable e) {
            LOG.error("Unable to reach workflow engine for deletion or " + "deletion failed", e);
            TransactionManager.rollback();
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    // Parallel update can get very clumsy if two feeds are updated which
    // are referred by a single process. Sequencing them.
    public synchronized APIResult update(HttpServletRequest request, String type,
                                         String entityName, String colo) {

        checkColo(colo);
        try {
            TransactionManager.startTransaction();
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            audit(request, entityName, type, "UPDATE");
            Entity oldEntity = getEntityObject(entityName, type);
            Entity newEntity = deserializeEntity(request, entityType);
            validate(newEntity);
            
            validateUpdate(oldEntity, newEntity);
            if (!oldEntity.deepEquals(newEntity)) {
                configStore.initiateUpdate(newEntity);
                getWorkflowEngine().update(oldEntity, newEntity);
                configStore.update(entityType, newEntity);
            }
            
            APIResult result = new APIResult(APIResult.Status.SUCCEEDED,
                    entityName + " updated successfully",
                    TransactionManager.getTransactionId());
            TransactionManager.commit();
            return result;
        } catch (Throwable e) {
            TransactionManager.rollback();
            LOG.error("Updation failed", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    private void validateUpdate(Entity oldEntity, Entity newEntity) throws IvoryException {
        if(oldEntity.getEntityType() != newEntity.getEntityType())
            throw new IvoryException(oldEntity.toShortString() +
                    " can't be updated with " + newEntity.toShortString());
            
        if(oldEntity.getEntityType() == EntityType.CLUSTER)
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
                throw new ValidationException(oldEntity.toShortString() +
                        ": " + prop + " can't be changed");
        }
    }

    private void canRemove(Entity entity) throws IvoryException {
        Pair<String, EntityType>[] referencedBy = EntityIntegrityChecker.referencedBy(entity);
        if (referencedBy != null && referencedBy.length > 0) {
        	StringBuffer messages= new StringBuffer();
        	for(Pair<String, EntityType> ref: referencedBy){
        		messages.append(ref).append("\n");
        	}
            throw new IvoryException(entity.getName() + "(" + entity.getEntityType() +
                    ") cant " + "be removed as it is referred by " + messages);
        }
    }

    protected Entity submitInternal(HttpServletRequest request, String type)
            throws IOException, IvoryException {

        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        Entity entity = deserializeEntity(request, entityType);
        
        ConfigurationStore configStore = ConfigurationStore.get();
        if(configStore.get(entityType, entity.getName()) != null)
            throw new EntityAlreadyExistsException(entity.toShortString() +
                    " already registered with configuration store. "
                    + "Can't be submitted again. Try removing before submitting.");

        validate(entity);
        configStore.publish(entityType, entity);
        LOG.info("Submit successful: (" + type + ")" + entity.getName());
        return entity;
    }

    protected Entity deserializeEntity(HttpServletRequest request, EntityType entityType)
            throws IOException, IvoryException {

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
    public String getStatus(String type, String entity) {

    	Entity entityObj = null;
        try {
            entityObj = getEntity(entity, type);
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            if (entityObj == null) {
                throw IvoryWebException.newException(new IvoryException(entity + "(" + 
                		type + ") is not present") , Response.Status.BAD_REQUEST);
            }
            if (entityType.isSchedulable()) {
                if (workflowEngine.isActive(entityObj)) {
                    if (workflowEngine.isSuspended(entityObj)) {
                        return EntityStatus.SUSPENDED.name();
                    } else {
                        return EntityStatus.RUNNING.name();
                    }
                } else {
                    return EntityStatus.SUBMITTED.name();
                }
            } else {
                return EntityStatus.SUBMITTED.name();
            }
        } catch (IvoryWebException e) {
        	throw e;
        } catch (Exception e) {
        	
            LOG.error("Unable to get status for entity " + entity + "(" + type + ")", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Returns the status of requested entity.
     * 
     * @param type
     * @param entity
     * @return String
     */
    public EntityList getDependencies(String type, String entity) {

        try {
            Entity entityObj = getEntity(entity, type);
            Set<Entity> dependents = EntityGraph.get().getDependents(entityObj);
            Entity[] entities = dependents.toArray(new Entity[dependents.size()]);
            return new EntityList(entities==null?new Entity[]{}:entities);
        }catch (Exception e) {
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
            if(entityNames == null || entityNames.equals(""))
            {
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
                throw new NoSuchElementException(entityName +
                        " (" + type + ") not found");
            }
            return entity.toString();
        } catch (Throwable e) {
            LOG.error("Unable to get entity definition from config " +
                    "store for (" + type + ") " + entityName, e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);

        }
    }

    protected Entity getEntityObject(String entity, String type) throws IvoryException {
        Entity entityObj = getEntity(entity, type);
        if (entityObj == null) {
            throw new NoSuchElementException(entity + " (" + type + ") not found");
        }
        return entityObj;
    }

    private Entity getEntity(String entity, String type) throws IvoryException {
        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        ConfigurationStore configStore = ConfigurationStore.get();
        return configStore.get(entityType, entity);
    }

    protected WorkflowEngine getWorkflowEngine() {
        return this.workflowEngine;
    }
}
