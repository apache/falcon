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

import org.apache.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("processinstance")
public class ProcessInstanceManager {

  private static Logger LOG = Logger.getLogger(ProcessInstanceManager.class);
  private static Logger AUDIT = Logger.getLogger("AUDIT");

  @GET
  @Path ("running/{process}")
  @Produces(MediaType.APPLICATION_JSON)
  public String getRunningInstances(@PathParam("process") String process,
                                    @QueryParam("partition") String partition) {
    return null;
  }

  @GET
  @Path ("status/{process}")
  @Produces(MediaType.TEXT_PLAIN)
  public String getStatus(@PathParam("process") String process,
                          @QueryParam("partition") String partition) {
    return null;
  }

  @POST
  @Path("kill/{process}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult killProcessInstance(@PathParam("process") String process,
                                       @QueryParam("partition") String
                                           partition) {
    return null;
  }

  @POST
  @Path("suspend/{process}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult suspendProcessInstance(@PathParam("process") String process,
                                          @QueryParam("partition") String
                                              partition) {
    return null;
  }

  @POST
  @Path("resume/{process}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult resumeProcessInstance(@PathParam("process") String process,
                                         @QueryParam("partition") String
                                             partition) {
    return null;
  }

  @POST
  @Path("rerun/{process}")
  @Produces(MediaType.APPLICATION_JSON)
  public APIResult reRunInstance(@PathParam("process") String process,
                                 @QueryParam("partition") String partition) {
    return null;
  }
}
