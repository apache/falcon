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
import java.util.Date;
import java.util.Properties;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.parser.ValidationException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.SchemaHelper;
import org.apache.ivory.logging.LogProvider;
import org.apache.ivory.resource.InstancesResult.Instance;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

public abstract class AbstractInstanceManager extends AbstractEntityManager {
    private static final Logger LOG = Logger.getLogger(AbstractInstanceManager.class);
	
    protected void checkType(String type) {
        if (StringUtils.isEmpty(type)) {
            throw IvoryWebException.newInstanceException("entity type is empty",
                    Response.Status.BAD_REQUEST);
        } else {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            if (entityType == EntityType.CLUSTER) {
                throw IvoryWebException.newInstanceException("Instance management functions don't apply to Cluster entities",
                        Response.Status.BAD_REQUEST);
            }
        }
    }

    public InstancesResult getRunningInstances(String type, String entity, String colo) {
        checkColo(colo);
        checkType(type);
        try {
            validateNotEmpty("entityName", entity);
            WorkflowEngine wfEngine = getWorkflowEngine();
            Entity entityObject = EntityUtil.getEntity(type, entity);
            return wfEngine.getRunningInstances(entityObject);
        } catch (Throwable e) {
            LOG.error("Failed to get running instances", e);
            throw IvoryWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }


    public InstancesResult getStatus(String type, String entity, String startStr, String endStr,
                                           String colo) {
        checkColo(colo);
        checkType(type);
        try {
			validateParams(type, entity, startStr, endStr);

			Date start = EntityUtil.parseDateUTC(startStr);
			Date end = getEndDate(start, endStr);
			Entity entityObject = EntityUtil.getEntity(type, entity);

			WorkflowEngine wfEngine = getWorkflowEngine();
			return wfEngine.getStatus(
					entityObject, start, end);
		} catch (Throwable e) {
			LOG.error("Failed to get instances status", e);
			throw IvoryWebException
					.newInstanceException(e, Response.Status.BAD_REQUEST);
		}
	}

	public InstancesResult getLogs(String type, String entity, String startStr,
			String endStr, String colo, String runId){

		try {
			// TODO getStatus does all validations and filters clusters
			InstancesResult result = getStatus(type, entity, startStr, endStr,
					colo);
			LogProvider logProvider = new LogProvider();
			Entity entityObject = EntityUtil.getEntity(type, entity);
			for (Instance instance : result.getInstances()) {
				logProvider.populateLogUrls(entityObject, instance, runId);
			}
			return result;
		} catch (Exception e) {
			LOG.error("Failed to get logs for instances", e);
			throw IvoryWebException.newInstanceException(e,
					Response.Status.BAD_REQUEST);
		}
	}

    public InstancesResult killInstance(HttpServletRequest request,
                                        String type, String entity, String startStr, String endStr, String colo) {

        checkColo(colo);
        checkType(type);
        try {
            audit(request, entity, type, "INSTANCE_KILL");
            validateParams(type, entity, startStr, endStr);
            
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);            
            Entity entityObject = EntityUtil.getEntity(type, entity);
            
            Properties props = getProperties(request);
            WorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.killInstances(entityObject, start, end, props);
        } catch (Throwable e) {
            LOG.error("Failed to kill instances", e);
            throw IvoryWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesResult suspendInstance(HttpServletRequest request,
                                           String type, String entity, String startStr, String endStr, String colo) {

        checkColo(colo);
        checkType(type);
        try {
            audit(request, entity, type, "INSTANCE_SUSPEND");
            validateParams(type, entity, startStr, endStr);
            
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);            
            Entity entityObject = EntityUtil.getEntity(type, entity);
            
            Properties props = getProperties(request);
            WorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.suspendInstances(entityObject, start, end, props);
        } catch (Throwable e) {
            LOG.error("Failed to suspend instances", e);
            throw IvoryWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesResult resumeInstance(HttpServletRequest request,
                                          String type, String entity, String startStr, String endStr, String colo) {

        checkColo(colo);
        checkType(type);
        try {
            audit(request, entity, type, "INSTANCE_RESUME");
            validateParams(type, entity, startStr, endStr);
            
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);            
            Entity entityObject = EntityUtil.getEntity(type, entity);
            
            Properties props = getProperties(request);
            WorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.resumeInstances(entityObject, start, end, props);
        } catch (Throwable e) {
            LOG.error("Failed to resume instances", e);
            throw IvoryWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesResult reRunInstance(String type, String entity, String startStr, String endStr,
                                                HttpServletRequest request, String colo) {

        checkColo(colo);
        checkType(type);
        try {
            audit(request, entity, type, "INSTANCE_RERUN");
            validateParams(type, entity, startStr, endStr);
            
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);            
            Entity entityObject = EntityUtil.getEntity(type, entity);

            Properties props = getProperties(request);
            WorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.reRunInstances(entityObject, start, end, props);
        } catch (Exception e) {
            LOG.error("Failed to rerun instances", e);
            throw IvoryWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    private Properties getProperties(HttpServletRequest request) throws IOException {
        Properties props = new Properties();
        ServletInputStream xmlStream = request==null?null:request.getInputStream();
        if (xmlStream != null) {
            if (xmlStream.markSupported()) {
                xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
            }
            props.load(xmlStream);
        }
        return props;
    }

    private Date getEndDate(Date start, String endStr) throws IvoryException {
        Date end;
        if (StringUtils.isEmpty(endStr)) {
            end = new Date(start.getTime() + 1000); // next sec
        } else
            end = EntityUtil.parseDateUTC(endStr);
        return end;
    }
    
    private void validateParams(String type, String entity, String startStr, String endStr) throws IvoryException {
        validateNotEmpty("entityType", type);
        validateNotEmpty("entityName", entity);
        validateNotEmpty("start", startStr);

        Entity entityObject = EntityUtil.getEntity(type, entity);
        validateDateRange(entityObject, startStr, endStr);
    }

    private void validateDateRange(Entity entity, String start, String end) throws IvoryException {
        IvoryException firstException = null;
        boolean valid = false;
        for (String cluster : EntityUtil.getClustersDefined(entity)) {
            try {
                validateDateRangeFor(entity, cluster, start, end);
                valid = true;
                break;
            } catch (IvoryException e) {
                if (firstException == null) firstException = e;
            }
        }
        if (!valid && firstException != null) throw firstException;

    }

    private void validateDateRangeFor(Entity entity, String cluster, String start, String end) throws IvoryException {
        Date clusterStart = EntityUtil.getStartTime(entity, cluster);
        Date clusterEnd = EntityUtil.getEndTime(entity, cluster);

        Date instStart = EntityUtil.parseDateUTC(start);
        if(instStart.before(clusterStart))
            throw new ValidationException("Start date " + start +
                    " is before" + entity.getEntityType() + "  start " + SchemaHelper.formatDateUTC(clusterStart) + " for cluster " + cluster);

        if(StringUtils.isNotEmpty(end)) {
            Date instEnd = EntityUtil.parseDateUTC(end);
            if(instStart.after(instEnd))
                throw new ValidationException("Start date " + start + " is after end date " + end + " for cluster " + cluster);

            if(instEnd.after(clusterEnd))
                throw new ValidationException("End date " + end + " is after " + entity.getEntityType() + " end " +
                        SchemaHelper.formatDateUTC(clusterEnd) + " for cluster " + cluster);
        } else if(instStart.after(clusterEnd))
            throw new ValidationException("Start date " + start + " is after " + entity.getEntityType() + " end " +
                    SchemaHelper.formatDateUTC(clusterEnd) + " for cluster " + cluster);
    }

    private void validateNotEmpty(String field, String param) throws ValidationException {
        if (StringUtils.isEmpty(param))
            throw new ValidationException("Parameter " + field + " is empty");
    }    
}
