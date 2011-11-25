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

import com.sun.jersey.api.core.HttpRequestContext;
import org.apache.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("entities")
public class EntityManager {

  private static Logger LOG = Logger.getLogger(EntityManager.class);
  private static Logger AUDIT = Logger.getLogger("AUDIT");

  @Context
  private HttpRequestContext requestContext;

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
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult submit(@PathParam("type") String type) {
    return null;
  }

  @POST
  @Path ("validate/{type}")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult validate(@PathParam("type") String type) {
    return null;
  }

  @POST
  @Path ("schedule/{type}/{entity}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult schedule(@PathParam("type") String type,
                            @PathParam("entity") String entity) {
    return null;
  }

  @POST
  @Path ("submitAndSchedule/{type}")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult submitAndSchedule(@PathParam("type") String type) {
    return null;
  }

  @DELETE
  @Path("delete/{type}/{entity}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult delete(@PathParam("type") String type,
                          @PathParam("entity") String entity) {
    return null;
  }

  @POST
  @Path("suspend/{type}/{entity}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult suspend(@PathParam("type") String type,
                          @PathParam("entity") String entity) {
    return null;
  }

  @POST
  @Path("resume/{type}/{entity}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult resume(@PathParam("type") String type,
                          @PathParam("entity") String entity) {
    return null;
  }

  @GET
  @Path("status/{type}/{entity}")
  @Produces(MediaType.TEXT_PLAIN)
  public String getStatus(@PathParam("type") String type,
                          @PathParam("entity") String entity) {
    return null;
  }

  @GET
  @Path("definition/{type}/{entity}")
  @Produces(MediaType.TEXT_XML)
  public String getEntityDefinition(@PathParam("type") String type,
                          @PathParam("entity") String entity) {
    return null;
  }

  //TODO: Entity information method ?
}
