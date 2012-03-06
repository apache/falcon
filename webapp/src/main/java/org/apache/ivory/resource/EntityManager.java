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
import java.util.NoSuchElementException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.io.IOUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.entity.parser.EntityParser;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityIntegrityChecker;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.workflow.WorkflowEngineFactory;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

public class EntityManager {

    private static final Logger LOG = Logger.getLogger(EntityManager.class);
    private static final Logger AUDIT = Logger.getLogger("AUDIT");
    protected static final int XML_DEBUG_LEN = 10 * 1024;

    private WorkflowEngine workflowEngine;
    protected ConfigurationStore configStore = ConfigurationStore.get();

    public EntityManager() {
        try {
            workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
        } catch (IvoryException e) {
            throw new IvoryRuntimException(e);
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
     * @param type
     *            - feed, process or data end point
     * @return result of the operation
     */
    @POST
    @Path("submit/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    public APIResult submit(@Context HttpServletRequest request,
                            @PathParam("type") String type) {

        try {
            audit(request, "STREAMED_DATA", type, "SUBMIT");
            submitInternal(request, type);
        } catch (Exception e) {
            LOG.error("Unable to persist entity object", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful");
    }

    /**
     * Post an entity XML with entity type. Validates the XML which can be
     * Process, Feed or Dataendpoint
     *
     * @param type
     * @return APIResule -Succeeded or Failed
     */
    @POST
    @Path("validate/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    public APIResult validate(@Context HttpServletRequest request,
                              @PathParam("type") String type) {

        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            deserializeEntity(request, entityType);
        } catch (Exception e) {
            LOG.error("Validation failed for entity (" + type + ") ", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "validate successful");
    }   
    
	 /**
     * Deletes a scheduled entity, a deleted entity is removed completely from
     * execution pool.
     *
     * @param type
     * @param entity
     * @return APIResult
     */
    @DELETE
    @Path("delete/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    public APIResult delete(@Context HttpServletRequest request,
                            @PathParam("type") String type,
                            @PathParam("entity") String entity) {
        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            audit(request, entity, type, "DELETE");
            String removedFromEngine = "";
            Entity entityObj = getEntityObject(entity, type);

            canRemove(entityType, entityObj);

            if (entityType.isSchedulable()) {
                if (getWorkflowEngine().isActive(entityObj)) {
                    getWorkflowEngine().delete(entityObj);
                    removedFromEngine = "(KILLED in ENGINE)";
                }
            }
            configStore.remove(entityType, entity);
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" +
                    type + ") removed successfully " + removedFromEngine);
        } catch (Exception e) {
            LOG.error("Unable to reach workflow engine for deletion or " +
                    "deletion failed", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    private void canRemove(EntityType entityType, Entity entityObj)
            throws IvoryException {

        Entity referencedBy = EntityIntegrityChecker.
                referencedBy(entityType, entityObj);
        if (referencedBy != null) {
            throw new IvoryException(entityObj.getName() + "(" + entityType + ") cant " +
                    "be removed as it is referred by " + referencedBy.getName() +
                    " (" + referencedBy.getEntityType() + ")");
        }
    }

    protected Entity submitInternal(HttpServletRequest request,
                                  String type)
            throws IOException, IvoryException {

        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        Entity entity = deserializeEntity(request, entityType);
        ConfigurationStore configStore = ConfigurationStore.get();
        configStore.publish(entityType, entity);
        LOG.info("Submit successful: (" + type + ")" + entity.getName());
        return entity;
    }

    private Entity deserializeEntity(HttpServletRequest request,
                                     EntityType entityType)
            throws IOException, IvoryException {

        EntityParser<?> entityParser = EntityParserFactory.getParser(entityType);
        InputStream xmlStream = request.getInputStream();
        if (xmlStream.markSupported()) {
            xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
        }
        try {
            return entityParser.parseAndValidate(xmlStream);
        } catch (IvoryException e) {
            if (LOG.isDebugEnabled() && xmlStream.markSupported()) {
                try {
                    xmlStream.reset();
                    String xmlData = getAsString(xmlStream);
                    LOG.debug("XML DUMP for (" + entityType + "): " + xmlData, e);
                } catch (IOException ignore) {}
            }
            throw e;
        }
    }

    private String getAsString(InputStream xmlStream) throws IOException {
        byte[] data = new byte[XML_DEBUG_LEN];
        IOUtils.readFully(xmlStream, data, 0, XML_DEBUG_LEN);
        return new String(data);
    }

   

    protected void audit(HttpServletRequest request, String entity,
                       String type, String action) {
        AUDIT.info("Performed " + action + " on " + entity + "(" + type +
                ") :: " + request.getRemoteHost() + "/" + request.getRemoteUser());
    }

    private enum EntityStatus {NOT_FOUND, NOT_SCHEDULED, SUSPENDED, ACTIVE}
    /**
     * Returns the status of requested entity.
     *
     * @param type
     * @param entity
     * @return String
     */
    @GET
    @Path("status/{type}/{entity}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getStatus(@PathParam("type") String type,
                            @PathParam("entity") String entity) {
        try {
            Entity entityObj = getEntity(entity, type);
            if (entityObj == null) {
                return EntityStatus.NOT_FOUND.name();
            }

            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            if (entityType.isSchedulable()) {
                if (workflowEngine.isActive(entityObj)) {
                    if (workflowEngine.isSuspended(entityObj)) {
                        return EntityStatus.SUSPENDED.name();
                    } else {
                        return EntityStatus.ACTIVE.name();
                    }
                } else {
                    return EntityStatus.NOT_SCHEDULED.name();
                }
            } else {
                return EntityStatus.NOT_SCHEDULED.name();
            }
        } catch (Exception e) {
            LOG.error("Unable to get status for entity " + entity + "(" + type + ")", e);
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
    @GET
    @Path("definition/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    public String getEntityDefinition(@PathParam("type") String type,
                                      @PathParam("entity") String entityName) {
        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            ConfigurationStore configStore = ConfigurationStore.get();
            Entity entity = configStore.get(entityType, entityName);
            if (entity == null) {
                throw new NoSuchElementException(entityName + " (" + type + ") not found");
            }
            return entity.toString();
        } catch (Exception e) {
            LOG.error("Unable to get entity definition from config store for (" +
                    type + ") " + entityName, e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);

        }
    }

    protected Entity getEntityObject(String entity, String type)
            throws IvoryException {
        Entity entityObj = getEntity(entity, type);
        if (entityObj == null) {
            throw new NoSuchElementException(entity + " (" + type + ") not found");
        }
        return entityObj;
    }

    private Entity getEntity(String entity, String type)
            throws IvoryException {
        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        ConfigurationStore configStore = ConfigurationStore.get();
        return configStore.get(entityType, entity);
    }
    
	protected WorkflowEngine getWorkflowEngine() {
		return this.workflowEngine;
	}
}
