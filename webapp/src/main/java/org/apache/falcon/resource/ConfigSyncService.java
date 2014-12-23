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

import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

/**
 * This class provides RESTful API for Entity Configurations.
 */
@Path("sync")
public class ConfigSyncService extends AbstractEntityManager {

    @POST
    @Path("submit/{type}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Monitored(event = "submit")
    @Override
    public APIResult submit(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("colo") @QueryParam("colo") String colo) {
        return super.submit(request, type, colo);
    }

    @DELETE
    @Path("delete/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Monitored(event = "delete")
    @Override
    public APIResult delete(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entity,
                            @Dimension("colo") @QueryParam("colo") String colo) {
        return super.delete(request, type, entity, colo);
    }

    @POST
    @Path("update/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Monitored(event = "update")
    @Override
    public APIResult update(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entityName,
                            @Dimension("colo") @QueryParam("colo") String colo) {
        return super.update(request, type, entityName, colo);
    }
}
