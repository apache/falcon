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

package org.apache.airavat.resource;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.airavat.entity.EntityType;
import org.apache.airavat.entity.parser.EntityParser;
import org.apache.airavat.entity.parser.EntityParserFactory;
import org.apache.log4j.Logger;

import com.sun.jersey.server.impl.model.method.dispatch.EntityParamDispatchProvider;

@Path("entities")
public class EntityManager {

  private static Logger LOG = Logger.getLogger(EntityManager.class);
  private static Logger AUDIT = Logger.getLogger("AUDIT");

  /**
   * Submit a new entity. Entities can be of type feed,
   * process or data end points. Entity definitions are
   * validated structurally against schema and subsequently
   * for other rules before they are admitted into the system
   *
   * Entity name acts as the key and an entity once added,
   * can't be added again unless deleted.
   *
   * @param type - feed, process or data end point
   * @return result of the operation
   */
  @POST
  @Path ("submit/{type}")
  @Consumes(MediaType.TEXT_XML)
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult submit(@PathParam("type") String type) {
    return null;
  }

  /**
   * Post an entity XML with entity in form field.
   * Validates the XML which can be Process, Feed or Dataendpoint 
   * 
   * @param type
   * @return APIResule -Succeeded or Failed
   */
  @POST
  @Path ("validate/{type}")
  @Consumes(MediaType.TEXT_XML)
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult validate(@Context javax.servlet.http.HttpServletRequest request, @PathParam("type") String type) {
	  
	  //TODO VALIDATE type and then proceed use custom validator or throw exception? 
     EntityParser epf = EntityParserFactory.getParser(EntityType.valueOf(type));
	  
     return null;
  }

  /**
   * Schedules an submitted entity immediately
   * 
   * @param type
   * @param entity
   * @return APIResult
   */
  @POST
  @Path ("schedule/{type}/{entity}")
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
  @Path ("submitAndSchedule/{type}")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult submitAndSchedule(@PathParam("type") String type) {
    return null;
  }

  /**
   * Deletes a scheduled entity, a deleted entity is removed completely from execution pool.
   * 
   * @param type
   * @param entity
   * @return
   */
  @DELETE
  @Path("delete/{type}/{entity}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult delete(@PathParam("type") String type,
                          @PathParam("entity") String entity) {
    return null;
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
    return null;
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
  @Produces(MediaType.TEXT_XML)
  public String getEntityDefinition(@PathParam("type") String type,
                          @PathParam("entity") String entity) {
    return null;
  }

  //TODO: Entity information method ?
}
