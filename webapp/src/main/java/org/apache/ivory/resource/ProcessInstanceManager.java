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

import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.Pair;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.parser.ValidationException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.resource.ProcessInstancesResult.WorkflowStatus;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

@Path("processinstance")
public class ProcessInstanceManager extends EntityManager {
    private static final Logger LOG = Logger.getLogger(ProcessInstanceManager.class);

    protected Process getProcess(String processName) throws IvoryException {
        Entity entity = getEntityObject(processName, EntityType.PROCESS.name());
        return (Process) entity;
    }
    
    @GET
    @Path("running/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProcessInstancesResult getRunningInstances(@PathParam("process") String processName) {
        try {
            validateNotEmpty("process", processName);
            WorkflowEngine wfEngine = getWorkflowEngine();
            Process process = getProcess(processName);
            Map<String, Set<String>> runInstances = wfEngine.getRunningInstances(process);
            return new ProcessInstancesResult("getRunningInstances is successful", runInstances.values().iterator().next(),
                    WorkflowStatus.RUNNING);
        } catch (Exception e) {
            LOG.error("Failed to get running instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("status/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProcessInstancesResult getStatus(@PathParam("process") String processName, @QueryParam("start") String startStr,
            @QueryParam("end") String endStr) {
        try {
            validateNotEmpty("process", processName);
            validateNotEmpty("start", startStr);
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);

            WorkflowEngine wfEngine = getWorkflowEngine();
            Process process = getProcess(processName);
            Map<String, Set<Pair<String, String>>> instances = wfEngine.getStatus(process, start, end);
            return new ProcessInstancesResult("getStatus is successful", instances.values().iterator().next());
        } catch (Exception e) {
            LOG.error("Failed to kill instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    private void validateNotEmpty(String field, String param) throws ValidationException {
        if (StringUtils.isEmpty(param))
            throw new ValidationException("Parameter " + field + " is empty");
    }

    @POST
    @Path("kill/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProcessInstancesResult killProcessInstance(@PathParam("process") String processName, @QueryParam("start") String startStr,
            @QueryParam("end") String endStr) {
        try {
            validateNotEmpty("process", processName);
            validateNotEmpty("start", startStr);
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);

            WorkflowEngine wfEngine = getWorkflowEngine();
            Process process = getProcess(processName);
            Map<String, Set<String>> killedInstances = wfEngine.killInstances(process, start, end);
            return new ProcessInstancesResult("killProcessInstance is successful", killedInstances.values().iterator().next(),
                    WorkflowStatus.KILLED);
        } catch (Exception e) {
            LOG.error("Failed to kill instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    private Date getEndDate(Date start, String endStr) throws IvoryException {
        Date end;
        if (StringUtils.isEmpty(endStr)) {
            end = new Date(start.getTime() + 1000); // next sec
        } else
            end = EntityUtil.parseDateUTC(endStr);
        return end;
    }

    @POST
    @Path("suspend/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    public APIResult suspendProcessInstance(@PathParam("process") String processName, @QueryParam("start") String startStr,
            @QueryParam("end") String endStr) {
        try {
            validateNotEmpty("process", processName);
            validateNotEmpty("start", startStr);
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);

            WorkflowEngine wfEngine = getWorkflowEngine();
            Process process = getProcess(processName);
            Map<String, Set<String>> suspendedInstances = wfEngine.suspendInstances(process, start, end);
            return new ProcessInstancesResult("suspendProcessInstance is successful", suspendedInstances.values().iterator().next(),
                    WorkflowStatus.SUSPENDED);
        } catch (Exception e) {
            LOG.error("Failed to suspend instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("resume/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    public APIResult resumeProcessInstance(@PathParam("process") String processName, @QueryParam("start") String startStr,
            @QueryParam("end") String endStr) {
        try {
            validateNotEmpty("process", processName);
            validateNotEmpty("start", startStr);
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);

            WorkflowEngine wfEngine = getWorkflowEngine();
            Process process = getProcess(processName);
            Map<String, Set<String>> resumedInstances = wfEngine.resumeInstances(process, start, end);
            return new ProcessInstancesResult("resumeProcessInstance is successful", resumedInstances.values().iterator().next(),
                    WorkflowStatus.RUNNING);
        } catch (Exception e) {
            LOG.error("Failed to suspend instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("rerun/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    public ProcessInstancesResult reRunInstance(@PathParam("process") String processName, @QueryParam("start") String startStr,
            @QueryParam("end") String endStr, @Context HttpServletRequest request) {
        try {
            validateNotEmpty("process", processName);
            validateNotEmpty("start", startStr);
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);
            Properties props = new Properties();
            ServletInputStream xmlStream = request.getInputStream();
            if (xmlStream != null) {
                if (xmlStream.markSupported()) {
                    xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
                }
                props.load(xmlStream);
            }

            WorkflowEngine wfEngine = getWorkflowEngine();
            Process process = getProcess(processName);
            Map<String, Set<String>> runInstances = wfEngine.reRunInstances(process, start, end, props);
            return new ProcessInstancesResult("reRunProcessInstance is successful", runInstances.values().iterator().next(),
                    WorkflowStatus.RUNNING);
        } catch (Exception e) {
            LOG.error("Failed to rerun instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }
}
