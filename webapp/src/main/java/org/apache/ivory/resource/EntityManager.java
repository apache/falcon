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
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;

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
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityIntegrityChecker;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.security.CurrentUser;
import org.apache.ivory.transaction.TransactionManager;
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
	@Monitored(event = "submit")
	public APIResult submit(@Context HttpServletRequest request,
			@Dimension("entityType") @PathParam("type") String type) {

		try {
			TransactionManager.startTransaction();
			audit(request, "STREAMED_DATA", type, "SUBMIT");
			Entity entity = submitInternal(request, type);
			APIResult result = new APIResult(APIResult.Status.SUCCEEDED,
					"Submit successful (" + type + ") " + entity.getName());
			TransactionManager.commit();
			return result;
		} catch (Throwable e) {
			LOG.error("Unable to persist entity object", e);
			TransactionManager.rollback();
			throw IvoryWebException
			.newException(e, Response.Status.BAD_REQUEST);
		}
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
			Entity entity = deserializeEntity(request, entityType);
			return new APIResult(APIResult.Status.SUCCEEDED,
					"Validated successfully (" + entityType + ") "
							+ entity.getName());
		} catch (Throwable e) {
			LOG.error("Validation failed for entity (" + type + ") ", e);
			throw IvoryWebException
			.newException(e, Response.Status.BAD_REQUEST);
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
	@DELETE
	@Path("delete/{type}/{entity}")
	@Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
	@Monitored(event = "delete")
	public APIResult delete(@Context HttpServletRequest request,
			@Dimension("entityType") @PathParam("type") String type,
			@Dimension("entityName") @PathParam("entity") String entity) {
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
			APIResult result = new APIResult(APIResult.Status.SUCCEEDED, entity
					+ "(" + type + ") removed successfully "
					+ removedFromEngine);
			TransactionManager.commit();
			return result;
		} catch (Throwable e) {
			LOG.error("Unable to reach workflow engine for deletion or "
					+ "deletion failed", e);
			TransactionManager.rollback();
			throw IvoryWebException
			.newException(e, Response.Status.BAD_REQUEST);
		}
	}

	@POST
	@Path("update/{type}/{entity}")
	@Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
	@Monitored(event = "update")
	// Parallel update can get very clumsy if two feeds are updated which
	// are referred by a single process. Sequencing them.
	public synchronized APIResult update(@Context HttpServletRequest request,
			@Dimension("entityType") @PathParam("type") String type,
			@Dimension("entityName") @PathParam("entity") String entityName) {
		try {
			TransactionManager.startTransaction();
			EntityType entityType = EntityType.valueOf(type.toUpperCase());
			audit(request, entityName, type, "UPDATE");
			Entity oldEntity = getEntityObject(entityName, type);
			Entity newEntity = deserializeEntity(request, entityType);
			if (!oldEntity.deepEquals(newEntity)) {
				if (entityType == EntityType.CLUSTER)
					throw new IvoryException(
							"Update not supported for clusters");

				validateUpdate(oldEntity, newEntity);
				configStore.initiateUpdate(newEntity);
				getWorkflowEngine().update(oldEntity, newEntity);
				configStore.update(entityType, newEntity);
			}
			APIResult result = new APIResult(APIResult.Status.SUCCEEDED,
					entityName + " updated successfully");
			TransactionManager.commit();
			return result;
		} catch (Throwable e) {
			TransactionManager.rollback();
			LOG.error("Updation failed", e);
			throw IvoryWebException
			.newException(e, Response.Status.BAD_REQUEST);
		}
	}

	private void validateUpdate(Entity oldEntity, Entity newEntity)
			throws IvoryException {
		String[] props = oldEntity.getImmutableProperties();
		for (String prop : props) {
			Object oldProp, newProp;
			try {
				oldProp = PropertyUtils.getProperty(oldEntity, prop);
				newProp = PropertyUtils.getProperty(newEntity, prop);
			} catch (Exception e) {
				throw new IvoryException(e);
			}
			if (!ObjectUtils.equals(oldProp, newProp))
				throw new ValidationException(prop + " can't be changed");
		}
	}

	private void canRemove(Entity entity) throws IvoryException {
		Pair<String, EntityType>[] referencedBy = EntityIntegrityChecker
				.referencedBy(entity);
		if (referencedBy != null && referencedBy.length > 0) {
			throw new IvoryException(entity.getName() + "("
					+ entity.getEntityType() + ") cant "
					+ "be removed as it is referred by " + referencedBy);
		}
	}

	protected Entity submitInternal(HttpServletRequest request, String type)
			throws IOException, IvoryException {
		EntityType entityType = EntityType.valueOf(type.toUpperCase());
		Entity entity = deserializeEntity(request, entityType);
		ConfigurationStore configStore = ConfigurationStore.get();
		configStore.publish(entityType, entity);
		LOG.info("Submit successful: (" + type + ")" + entity.getName());
		return entity;
	}

	private Entity deserializeEntity(HttpServletRequest request,
			EntityType entityType) throws IOException, IvoryException {

		EntityParser<?> entityParser = EntityParserFactory
				.getParser(entityType);
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
					LOG.debug("XML DUMP for (" + entityType + "): " + xmlData,
							e);
				} catch (IOException ignore) {
				}
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

		if (request == null) {
			return; // this must be internal call from Ivory
		}
		AUDIT.info("Performed " + action + " on " + entity + "(" + type
				+ ") :: " + request.getRemoteHost() + "/"
				+ CurrentUser.getUser());
	}

	private enum EntityStatus {
		NOT_FOUND, NOT_SCHEDULED, SUSPENDED, ACTIVE
	}

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
	@Monitored(event = "status")
	public String getStatus(
			@Dimension("entityType") @PathParam("type") String type,
			@Dimension("entityName") @PathParam("entity") String entity) {
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
			LOG.error("Unable to get status for entity " + entity + "(" + type
					+ ")", e);
			throw IvoryWebException
			.newException(e, Response.Status.BAD_REQUEST);
		}
	}

	/**
	 * Returns the status of requested entity.
	 * 
	 * @param type
	 * @param entity
	 * @return String
	 */
	@GET
	@Path("dependencies/{type}/{entity}")
	@Produces(MediaType.TEXT_XML)
	@Monitored(event = "dependencies")
	public EntityList getDependencies(
			@Dimension("entityType") @PathParam("type") String type,
			@Dimension("entityName") @PathParam("entity") String entity) {
		try {
			Entity entityObj = getEntity(entity, type);
			Set<Entity> dependents = EntityGraph.get().getDependents(entityObj);
			Entity[] entities = dependents
					.toArray(new Entity[dependents.size()]);
			return new EntityList(entities);
		} catch (Exception e) {
			LOG.error("Unable to get dependencies for entity " + entity + "("
					+ type + ")", e);
			throw IvoryWebException
			.newException(e, Response.Status.BAD_REQUEST);
		}
	}

	/**
	 * Returns the list of entities registered of a given type.
	 * 
	 * @param type
	 * @return String
	 */
	@GET
	@Path("list/{type}")
	@Produces(MediaType.TEXT_XML)
	public EntityList getDependencies(@PathParam("type") String type) {
		try {
			EntityType entityType = EntityType.valueOf(type.toUpperCase());
			Collection<String> entityNames = configStore
					.getEntities(entityType);
			Entity[] entities = new Entity[entityNames.size()];
			int index = 0;
			for (String entityName : entityNames) {
				entities[index++] = configStore.get(entityType, entityName);
			}
			return new EntityList(entities);
		} catch (Exception e) {
			LOG.error("Unable to get list for entities for (" + type + ")", e);
			throw IvoryWebException
			.newException(e, Response.Status.BAD_REQUEST);
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
				throw new NoSuchElementException(entityName + " (" + type
						+ ") not found");
			}
			return entity.toString();
		} catch (Throwable e) {
			LOG.error("Unable to get entity definition from config store for ("
					+ type + ") " + entityName, e);
			throw IvoryWebException
			.newException(e, Response.Status.BAD_REQUEST);

		}
	}

	public Entity getEntityObject(String entity, String type)
			throws IvoryException {
		Entity entityObj = getEntity(entity, type);
		if (entityObj == null) {
			throw new NoSuchElementException(entity + " (" + type
					+ ") not found");
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
