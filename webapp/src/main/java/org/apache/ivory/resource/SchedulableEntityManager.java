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
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.ivory.IvoryWebException;
import org.apache.ivory.entity.v0.UnschedulableEntityException;
import org.apache.ivory.entity.store.EntityAlreadyExistsException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.log4j.Logger;

/**
 * REST resource of allowed actions on Schedulable Entities Only Process and
 * Feed can have schedulable actions
 */
@Path("entities")
public class SchedulableEntityManager extends EntityManager {

	private static final Logger LOG = Logger
			.getLogger(SchedulableEntityManager.class);

	/**
	 * Schedules an submitted entity immediately
	 * 
	 * @param type
	 * @param entity
	 * @return APIResult
	 */
	@POST
	@Path("schedule/{type}/{entity}")
	@Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
	public APIResult schedule(@Context HttpServletRequest request,
			@PathParam("type") String type, @PathParam("entity") String entity) {

		try {
			checkSchedulableEntity(type);
			audit(request, entity, type, "SCHDULED");
			Entity entityObj = getEntityObject(entity, type);
			if (!getWorkflowEngine().isActive(entityObj)) {
				getWorkflowEngine().schedule(entityObj);
				return new APIResult(APIResult.Status.SUCCEEDED, entity + "("
						+ type + ") scheduled successfully");
			} else {
				throw new EntityAlreadyExistsException(entity + "(" + type
						+ ") is already scheduled with " + "workflow engine");
			}
		} catch (Exception e) {
			LOG.error("Unable to schedule workflow", e);
			throw IvoryWebException
					.newException(e, Response.Status.BAD_REQUEST);
		}
	}

	/**
	 * Submits a new entity and schedules it immediately
	 * 
	 * @param type
	 * @return
	 */
	@POST
	@Path("submitAndSchedule/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
	public APIResult submitAndSchedule(@Context HttpServletRequest request,
			@PathParam("type") String type) {
		try {
			checkSchedulableEntity(type);
			audit(request, "STREAMED_DATA", type, "SUBMIT_AND_SCHEDULE");
			Entity entity = submitInternal(request, type);
			return schedule(request, type, entity.getName());
		} catch (Exception e) {
			LOG.error("Unable to submit and schedule ", e);
			throw IvoryWebException
					.newException(e, Response.Status.BAD_REQUEST);
		}
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
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    public APIResult suspend(@Context HttpServletRequest request,
                             @PathParam("type") String type,
                             @PathParam("entity") String entity) {

        try {
			checkSchedulableEntity(type);
            audit(request, entity, type, "SUSPEND");
            Entity entityObj = getEntityObject(entity, type);
            getWorkflowEngine().suspend(entityObj);
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" +
                    type + ") suspended successfully");
        } catch (Exception e) {
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
    @POST
    @Path("resume/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    public APIResult resume(@Context HttpServletRequest request,
                            @PathParam("type") String type,
                            @PathParam("entity") String entity) {

        try {
			checkSchedulableEntity(type);
            audit(request, entity, type, "RESUME");
            Entity entityObj = getEntityObject(entity, type);
            getWorkflowEngine().resume(entityObj);
            return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" +
                    type + ") resumed successfully");
        } catch (Exception e) {
            LOG.error("Unable to resume entity", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }    

	private void checkSchedulableEntity(String type) throws UnschedulableEntityException {
		EntityType entityType = EntityType.valueOf(type.toUpperCase());
		if (!entityType.isSchedulable()) {
			throw new UnschedulableEntityException("Entity type (" + type + ") "
					+ " cannot be Scheduled/Suspended/Resumed");
		}
	}
}
