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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.UnschedulableEntityException;
import org.apache.ivory.monitors.Dimension;
import org.apache.log4j.Logger;

/**
 * REST resource of allowed actions on Schedulable Entities Only Process and
 * Feed can have schedulable actions
 */
public abstract class AbstractSchedulableEntityManager extends AbstractEntityManager {

    private static final Logger LOG = Logger.getLogger(AbstractSchedulableEntityManager.class);

    /**
     * Schedules an submitted entity immediately
     * 
     * @param type
     * @param entity
     * @return APIResult
     */
    public APIResult schedule(@Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("entityName") @PathParam("entity") String entity, @Dimension("colo") @PathParam("colo") String colo) {
        checkColo(colo);
        try {
            audit(request, entity, type, "SCHEDULED");
            scheduleInternal(type, entity);
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + type + ") scheduled successfully");
        } catch (Throwable e) {
            LOG.error("Unable to schedule workflow", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    private synchronized void scheduleInternal(String type, String entity) throws IvoryException {
        checkSchedulableEntity(type);
        Entity entityObj = getEntity(entity, type);
        if (!getWorkflowEngine().isActive(entityObj))
            getWorkflowEngine().schedule(entityObj);
    }

    /**
     * Submits a new entity and schedules it immediately
     * 
     * @param type
     * @return
     */
    public APIResult submitAndSchedule(@Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("colo") @PathParam("colo") String colo) {
        checkColo(colo);
        try {
            checkSchedulableEntity(type);
            audit(request, "STREAMED_DATA", type, "SUBMIT_AND_SCHEDULE");
            Entity entity = submitInternal(request, type);
            scheduleInternal(type, entity.getName());
            return new APIResult(APIResult.Status.SUCCEEDED, entity.getName() + "(" + type + ") scheduled successfully");
        } catch (Throwable e) {
            LOG.error("Unable to submit and schedule ", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Suspends a running entity
     * 
     * @param type
     * @param entity
     * @return APIResult
     */
    public APIResult suspend(@Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("entityName") @PathParam("entity") String entity, @Dimension("entityName") @PathParam("entity") String colo) {
        checkColo(colo);
        try {
            checkSchedulableEntity(type);
            audit(request, entity, type, "SUSPEND");
            Entity entityObj = getEntity(entity, type);
            if (getWorkflowEngine().isActive(entityObj))
                getWorkflowEngine().suspend(entityObj);
            else
                throw new IvoryException(entity + "(" + type + ") is not scheduled");
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + type + ") suspended successfully");
        } catch (Throwable e) {
            LOG.error("Unable to suspend entity", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Resumes a suspended entity
     * 
     * @param type
     * @param entity
     * @return APIResult
     */
    public APIResult resume(@Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("entityName") @PathParam("entity") String entity, @Dimension("colo") @PathParam("colo") String colo) {

        checkColo(colo);
        try {
            checkSchedulableEntity(type);
            audit(request, entity, type, "RESUME");
            Entity entityObj = getEntity(entity, type);
            if(getWorkflowEngine().isActive(entityObj))
                getWorkflowEngine().resume(entityObj);
            else
                throw new IvoryException(entity + "(" + type + ") is not scheduled");
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + type + ") resumed successfully");
        } catch (Throwable e) {
            LOG.error("Unable to resume entity", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    private void checkSchedulableEntity(String type) throws UnschedulableEntityException {
        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        if (!entityType.isSchedulable()) {
            throw new UnschedulableEntityException("Entity type (" + type + ") " + " cannot be Scheduled/Suspended/Resumed");
        }
    }
}