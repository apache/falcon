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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.logging.LogProvider;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.log4j.Logger;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

/**
 * A base class for managing Entity's Instance operations.
 */
public abstract class AbstractInstanceManager extends AbstractEntityManager {
    private static final Logger LOG = Logger.getLogger(AbstractInstanceManager.class);

    protected void checkType(String type) {
        if (StringUtils.isEmpty(type)) {
            throw FalconWebException.newInstanceException("entity type is empty",
                    Response.Status.BAD_REQUEST);
        } else {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            if (entityType == EntityType.CLUSTER) {
                throw FalconWebException.newInstanceException(
                        "Instance management functions don't apply to Cluster entities",
                        Response.Status.BAD_REQUEST);
            }
        }
    }

    public InstancesResult getRunningInstances(String type, String entity, String colo) {
        checkColo(colo);
        checkType(type);
        try {
            validateNotEmpty("entityName", entity);
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            Entity entityObject = EntityUtil.getEntity(type, entity);
            return wfEngine.getRunningInstances(entityObject);
        } catch (Throwable e) {
            LOG.error("Failed to get running instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
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

            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.getStatus(
                    entityObject, start, end);
        } catch (Throwable e) {
            LOG.error("Failed to get instances status", e);
            throw FalconWebException
                    .newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesSummaryResult getSummary(String type, String entity, String startStr, String endStr, String colo) {
        checkColo(colo);
        checkType(type);
        try {
            validateParams(type, entity, startStr, endStr);

            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);
            Entity entityObject = EntityUtil.getEntity(type, entity);

            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.getSummary(entityObject, start, end);
        } catch (Throwable e) {
            LOG.error("Failed to get instances status", e);
            throw FalconWebException.newInstanceSummaryException(e, Response.Status.BAD_REQUEST);
        }
    }

    public InstancesResult getLogs(String type, String entity, String startStr,
                                   String endStr, String colo, String runId) {
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
            throw FalconWebException.newInstanceException(e,
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
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.killInstances(entityObject, start, end, props);
        } catch (Throwable e) {
            LOG.error("Failed to kill instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
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
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.suspendInstances(entityObject, start, end, props);
        } catch (Throwable e) {
            LOG.error("Failed to suspend instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
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
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.resumeInstances(entityObject, start, end, props);
        } catch (Throwable e) {
            LOG.error("Failed to resume instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
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
            AbstractWorkflowEngine wfEngine = getWorkflowEngine();
            return wfEngine.reRunInstances(entityObject, start, end, props);
        } catch (Exception e) {
            LOG.error("Failed to rerun instances", e);
            throw FalconWebException.newInstanceException(e, Response.Status.BAD_REQUEST);
        }
    }

    private Properties getProperties(HttpServletRequest request) throws IOException {
        Properties props = new Properties();
        ServletInputStream xmlStream = request == null ? null : request.getInputStream();
        if (xmlStream != null) {
            if (xmlStream.markSupported()) {
                xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
            }
            props.load(xmlStream);
        }
        return props;
    }

    private Date getEndDate(Date start, String endStr) throws FalconException {
        Date end;
        if (StringUtils.isEmpty(endStr)) {
            end = new Date(start.getTime() + 1000); // next sec
        } else {
            end = EntityUtil.parseDateUTC(endStr);
        }
        return end;
    }

    private void validateParams(String type, String entity, String startStr, String endStr) throws FalconException {
        validateNotEmpty("entityType", type);
        validateNotEmpty("entityName", entity);
        validateNotEmpty("start", startStr);

        Entity entityObject = EntityUtil.getEntity(type, entity);
        validateDateRange(entityObject, startStr, endStr);
    }

    private void validateDateRange(Entity entity, String start, String end) throws FalconException {
        Set<String> clusters = EntityUtil.getClustersDefined(entity);
        Pair<Date, String> clusterMinStartDate = null;
        Pair<Date, String> clusterMaxEndDate = null;
        for (String cluster : clusters) {
            if (clusterMinStartDate == null || clusterMinStartDate.first.after(
                    EntityUtil.getStartTime(entity, cluster))) {
                clusterMinStartDate = Pair.of(EntityUtil.getStartTime(entity, cluster), cluster);
            }
            if (clusterMaxEndDate == null || clusterMaxEndDate.first.before(EntityUtil.getEndTime(entity, cluster))) {
                clusterMaxEndDate = Pair.of(EntityUtil.getEndTime(entity, cluster), cluster);
            }
        }

        validateDateRangeFor(entity, clusterMinStartDate, clusterMaxEndDate,
                start, end);
    }

    private void validateDateRangeFor(Entity entity, Pair<Date, String> clusterMinStart,
                                      Pair<Date, String> clusterMaxEnd, String start, String end)
        throws FalconException {

        Date instStart = EntityUtil.parseDateUTC(start);
        if (instStart.before(clusterMinStart.first)) {
            throw new ValidationException("Start date " + start + " is before "
                    + entity.getEntityType() + "'s  start "
                    + SchemaHelper.formatDateUTC(clusterMinStart.first)
                    + " for cluster " + clusterMinStart.second);
        }

        if (StringUtils.isNotEmpty(end)) {
            Date instEnd = EntityUtil.parseDateUTC(end);
            if (instStart.after(instEnd)) {
                throw new ValidationException("Start date " + start
                        + " is after end date " + end);
            }

            if (instEnd.after(clusterMaxEnd.first)) {
                throw new ValidationException("End date " + end + " is after "
                        + entity.getEntityType() + "'s end "
                        + SchemaHelper.formatDateUTC(clusterMaxEnd.first)
                        + " for cluster " + clusterMaxEnd.second);
            }
        } else if (instStart.after(clusterMaxEnd.first)) {
            throw new ValidationException("Start date " + start + " is after "
                    + entity.getEntityType() + "'s end "
                    + SchemaHelper.formatDateUTC(clusterMaxEnd.first)
                    + " for cluster " + clusterMaxEnd.second);
        }
    }

    private void validateNotEmpty(String field, String param) throws ValidationException {
        if (StringUtils.isEmpty(param)) {
            throw new ValidationException("Parameter " + field + " is empty");
        }
    }
}
