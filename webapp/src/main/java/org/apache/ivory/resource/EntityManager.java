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
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.parser.EntityParser;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.dataset.Dataset;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.mappers.CoordinatorMapper;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.log4j.Logger;

@Path("entities")
public class EntityManager {

	private static final Logger LOG = Logger.getLogger(EntityManager.class);
	private static final Logger AUDIT = Logger.getLogger("AUDIT");

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
	public APIResult submit(
			@Context javax.servlet.http.HttpServletRequest request,
			@PathParam("type") String type) {

		try {
			EntityType entityType = EntityType.valueOf(type.toUpperCase());
			EntityParser<?> entityParser = EntityParserFactory
					.getParser(entityType);
			InputStream xmlStream = request.getInputStream();
			Entity entity = entityParser.parse(xmlStream);
			ConfigurationStore configStore = ConfigurationStore.get();
			Entity existingEntity = configStore.get(entityType, entity.getName());
			if(existingEntity!=null){
				LOG.error(entity.getName()+" already exists");
				return new APIResult(APIResult.Status.FAILED, entity.getName()+" already exists");	
			}
			
			Map<Entity,EntityType> entityMap = new LinkedHashMap<Entity,EntityType>();
			
			// TODO Move this code to seperate class
			if (entityType.equals(EntityType.PROCESS)) {
				Process process = (Process) entity;
				entityMap.put(process, EntityType.PROCESS);
				for (Input input : process.getInputs().getInput()) {
					String feedName = input.getFeed();
					Dataset dataset = configStore.get(EntityType.DATASET, feedName);
					if (dataset == null) {
						LOG.error("Referenced input feed: " + feedName
								+ " does not exist in config Store");
						return new APIResult(APIResult.Status.FAILED,
								"Referenced input feed: " + feedName
										+ " does not exist in config Store");
					}
					entityMap.put(dataset, EntityType.DATASET);
				}
				for (Output output : process.getOutputs().getOutput()) {
					String feedName = output.getFeed();
					Dataset dataset = configStore.get(EntityType.DATASET, feedName);
					if (dataset == null) {
						LOG.error("Referenced output feed: " + feedName
								+ " does not exist in config Store");
						return new APIResult(APIResult.Status.FAILED,
								"Referenced output feed: " + feedName
										+ " does not exist in config Store");
					}
					entityMap.put(dataset, EntityType.DATASET);
				}
					//Finally get the coordinator based on submited entities
					COORDINATORAPP coordinatorapp = new COORDINATORAPP();
					CoordinatorMapper coordinateMapper = new CoordinatorMapper(
							entityMap, coordinatorapp);
					coordinateMapper.mapToDefaultCoordinator();
					ObjectFactory objectFactory = new ObjectFactory();
					JAXBElement<COORDINATORAPP> app = objectFactory.createCoordinatorApp(coordinatorapp);
					StringWriter stringWriter = new StringWriter();
					Marshaller marshaller;
					
					try {
						marshaller = Util.getMarshaller(COORDINATORAPP.class);
						marshaller.marshal(app, stringWriter);
						System.out.println(stringWriter.toString());
					} catch (JAXBException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					//THE coordinator will be populated now
			}
			
			configStore.publish(entityType, entity);
			LOG.info("Submit successful: " + entity.getName());
		} catch (IvoryException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage());
		} catch (IllegalArgumentException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage());
		} catch (IOException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage());
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
	public APIResult validate(
			@Context javax.servlet.http.HttpServletRequest request,
			@PathParam("type") String type) {

		try {
			EntityType entityType = EntityType.valueOf(type.toUpperCase());
			EntityParser<?> entityParser = EntityParserFactory
					.getParser(entityType);
			InputStream xmlStream = request.getInputStream();
			entityParser.validateSchema(xmlStream);
			LOG.info("Validate successful");
		} catch (IOException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage());
		} catch (IllegalArgumentException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage());
		} catch (IvoryException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage());
		}

		return new APIResult(APIResult.Status.SUCCEEDED, "Validate successful");
	}

	/**
	 * Schedules an submitted entity immediately
	 * 
	 * @param type
	 * @param entity
	 * @return APIResult
	 */
	@POST
	@Path("schedule/{type}/{entity}")
	@Produces(MediaType.APPLICATION_JSON)
	public APIResult schedule(@PathParam("type") String type,
			@PathParam("entity") String entity) {
		return null;
	}

	/**
	 * Submits a new entity and schedules it immediately
	 * 
	 * @param type
	 * @return
	 */
	@POST
	@Path("submitAndSchedule/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	public APIResult submitAndSchedule(@PathParam("type") String type) {
		return null;
	}

	/**
	 * Deletes a scheduled entity, a deleted entity is removed completely from
	 * execution pool.
	 * 
	 * @param type
	 * @param entity
	 * @return
	 */
	@DELETE
	@Path("delete/{type}/{entity}")
	@Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
	public APIResult delete(@PathParam("type") String type,
			@PathParam("entity") String entity) {
		try {
			EntityType entityType = EntityType.valueOf(type.toUpperCase());
			ConfigurationStore configStore = ConfigurationStore.get();
			boolean isRemoved = configStore.remove(entityType, entity);
			if (isRemoved == false) {
				return new APIResult(APIResult.Status.FAILED, "Entity: "
						+ entity + " does not exists");
			}
		} catch (IllegalArgumentException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage());
		} catch (StoreAccessException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage());
		}
		return new APIResult(APIResult.Status.SUCCEEDED, "Delete successful");
	}

	/**
	 * Suspends a running entity
	 * 
	 * @param type
	 * @param entity
	 * @return APIResult
	 */
	@POST
	@Path("suspend/{type}/{entity}")
	@Produces(MediaType.APPLICATION_JSON)
	public APIResult suspend(@PathParam("type") String type,
			@PathParam("entity") String entity) {
		return null;
	}

	/**
	 * Resumes a suspended entity
	 * 
	 * @param type
	 * @param entity
	 * @return APIResult
	 */
	@POST
	@Path("resume/{type}/{entity}")
	@Produces(MediaType.APPLICATION_JSON)
	public APIResult resume(@PathParam("type") String type,
			@PathParam("entity") String entity) {
		return null;
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
	public String getStatus(@PathParam("type") String type,
			@PathParam("entity") String entity) {
		return "hello Hi ...\n";
	}

	/**
	 * Returns the entity definition as an XML based on name
	 * 
	 * @param type
	 * @param entity
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
			Entity entity= configStore.get(entityType, entityName);
			if(entity==null){
				LOG.error(entityName+" does not exists");
				return new APIResult(APIResult.Status.FAILED, entityName+" does not exists").toString();	
			}
			LOG.info("Returned entity: " + entity);
			return entity.toString();
		} catch (IllegalArgumentException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage()).toString();
		} catch (StoreAccessException e) {
			LOG.error(e.getMessage());
			return new APIResult(APIResult.Status.FAILED, e.getMessage()).toString();
		}
	}
}
