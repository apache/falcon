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

import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataParam;
import org.apache.falcon.FalconWebException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.POST;
import javax.ws.rs.Consumes;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.List;

/**
 * This class provides RESTful API for the extensions.
 */
@Path("extension")
public class ExtensionManager extends AbstractExtensionManager {
    private static final Logger LOG = LoggerFactory.getLogger(ExtensionManager.class);

    @GET
    @Path("enumerate")
    @Produces({MediaType.APPLICATION_JSON})
    public APIResult getExtensions() {
        LOG.error("Enumerate is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Enumerate is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @POST
    @Path("schedule/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult schedule(@PathParam("job-name") String jobName,
                              @Context HttpServletRequest request,
                              @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        LOG.error("schedule is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("schedule is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @POST
    @Path("submit/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.MULTIPART_FORM_DATA,
            MediaType.APPLICATION_OCTET_STREAM})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult submit(
            @PathParam("extension-name") String extensionName,
            @Context HttpServletRequest request,
            @DefaultValue("") @QueryParam("doAs") String doAsUser,
            @QueryParam("jobName") String jobName,
            @FormDataParam("processes") List<FormDataBodyPart> processForms,
            @FormDataParam("feeds") List<FormDataBodyPart> feedForms,
            @FormDataParam("config") InputStream config) {
        LOG.error("submit is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("submit is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @POST
    @Path("submitAndSchedule/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.MULTIPART_FORM_DATA})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult submitAndSchedule(
            @PathParam("extension-name") String extensionName,
            @Context HttpServletRequest request,
            @DefaultValue("") @QueryParam("doAs") String doAsUser,
            @QueryParam("jobName") String jobName,
            @FormDataParam("processes") List<FormDataBodyPart> processForms,
            @FormDataParam("feeds") List<FormDataBodyPart> feedForms,
            @FormDataParam("config") InputStream config) {
        LOG.error("submitAndSchedule is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("submitAndSchedule is not supported on Server. Please run your "
                + "operation on Prism.");
    }

    @GET
    @Path("describe/{extension-name}")
    @Produces(MediaType.TEXT_PLAIN)
    public APIResult getExtensionDescription(
            @PathParam("extension-name") String extensionName) {
        LOG.error("Describe is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Describe is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @GET
    @Path("detail/{extension-name}")
    @Produces({MediaType.APPLICATION_JSON})
    public APIResult getDetail(@PathParam("extension-name") String extensionName) {
        LOG.error("Detail is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Detail is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @POST
    @Path("unregister/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces(MediaType.TEXT_PLAIN)
    public APIResult deleteExtensionMetadata(
            @PathParam("extension-name") String extensionName) {
        LOG.error("Unregister is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Unregister is not supported on Server. Please run your operation "
                + "on Prism.");
    }

    @POST
    @Path("suspend/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult suspend(@PathParam("job-name") String jobName,
                             @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        LOG.error("Suspend of an extension job is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Suspend of an extension job is not supported on Server."
                + "Please run your operation on Prism.");
    }

    @POST
    @Path("resume/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult resume(@PathParam("job-name") String jobName,
                            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        LOG.error("Resume of an extension job is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Resume of an extension job is not supported on Server."
                + "Please run your operation on Prism.");
    }

    @GET
    @Path("definition/{extension-name}")
    @Produces({MediaType.APPLICATION_JSON})
    public APIResult getExtensionDefinition(
            @PathParam("extension-name") String extensionName) {
        LOG.error("Definition is not supported on Server.Please run your operation on Prism ");
        throw FalconWebException.newAPIException("Definition is not supported on Server. Please run your operation "
                + "on Prism.");
    }
}
