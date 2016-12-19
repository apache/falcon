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

import org.apache.falcon.FalconWebException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.POST;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class provides RESTful API for the extensions.
 */
@Path("extension")
public class ExtensionManager {
    public static final Logger LOG = LoggerFactory.getLogger(ExtensionManager.class);

    @GET
    @Path("enumerate")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getExtensions() {
        LOG.error("Enumerate is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Enumerate is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @GET
    @Path("describe/{extension-name}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getExtensionDescription(
            @PathParam("extension-name") String extensionName) {
        LOG.error("Describe is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Describe is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @GET
    @Path("detail/{extension-name}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getDetail(@PathParam("extension-name") String extensionName) {
        LOG.error("Detail is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Detail is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @POST
    @Path("unregister/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces(MediaType.TEXT_PLAIN)
    public String deleteExtensionMetadata(
            @PathParam("extension-name") String extensionName){
        LOG.error("Unregister is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Unregister is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @GET
    @Path("definition/{extension-name}")
    @Produces({MediaType.APPLICATION_JSON})
    public String getExtensionDefinition(
            @PathParam("extension-name") String extensionName) {
        LOG.error("Definition is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Definition is not supported on Server. Please run your operation "
                + "on Prism.");
    }
}
