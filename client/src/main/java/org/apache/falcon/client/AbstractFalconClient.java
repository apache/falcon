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
package org.apache.falcon.client;

import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;

import java.io.IOException;
import java.util.List;

/**
 * Abstract Client API to submit and manage Falcon Entities (Cluster, Feed, Process) jobs
 * against an Falcon instance.
 */
public abstract class AbstractFalconClient {

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck

    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param filePath Path for the entity definition
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult submit(String entityType, String filePath, String doAsUser) throws FalconCLIException,
            IOException;

    /**
     * Schedules an submitted process entity immediately.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param colo Cluster name.
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult schedule(EntityType entityType, String entityName, String colo, Boolean skipDryRun,
                                        String doAsuser, String properties) throws FalconCLIException;

    /**
     * Delete the specified entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param doAsUser Proxy User.
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult delete(EntityType entityType, String entityName,
                                     String doAsUser) throws FalconCLIException;

    /**
     * Validates the submitted entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param filePath Path for the entity definition to validate.
     * @param skipDryRun Dry run.
     * @param doAsUser Proxy User.
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult validate(String entityType, String filePath, Boolean skipDryRun,
                                       String doAsUser) throws FalconCLIException;

    /**
     * Updates the submitted entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param filePath Path for the entity definition to update.
     * @param skipDryRun Dry run.
     * @param doAsUser Proxy User.
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult update(String entityType, String entityName, String filePath,
                                                       Boolean skipDryRun, String doAsUser) throws FalconCLIException;

    /**
     * Get definition of the entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param doAsUser Proxy user.
     * @return
     * @throws FalconCLIException
     */
    public abstract Entity getDefinition(String entityType, String entityName,
                                         String doAsUser) throws FalconCLIException;



    /**
     *
     * @param type entity type
     * @param entity entity name
     * @param start start time
     * @param end end time
     * @param colo colo name
     * @param lifeCycles lifecycle of an entity (for ex : feed has replication,eviction).
     * @param filterBy filter operation can be applied to results
     * @param orderBy
     * @param sortOrder sort order can be asc or desc
     * @param offset offset while displaying results
     * @param numResults num of Results to output
     * @param doAsUser
     * @return
     * @throws FalconCLIException
     */
    public abstract InstancesResult getStatusOfInstances(String type, String entity,
                                                         String start, String end,
                                                         String colo, List<LifeCycle> lifeCycles, String filterBy,
                                                         String orderBy, String sortOrder,
                                                         Integer offset, Integer numResults,
                                                         String doAsUser) throws FalconCLIException;
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    /**
     * Suspend an entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Status of the entity.
     * @throws FalconCLIException
     */
    public abstract APIResult suspend(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException;

    /**
     * Resume a supended entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Result of the resume command.
     * @throws FalconCLIException
     */
    public abstract APIResult resume(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException;

    /**
     * Get status of the entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Status of the entity.
     * @throws FalconCLIException
     */
    public abstract APIResult getStatus(EntityType entityType, String entityName, String colo, String doAsUser) throws
            FalconCLIException;
}
