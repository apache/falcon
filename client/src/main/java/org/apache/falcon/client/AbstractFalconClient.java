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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.APIResult;

import java.io.IOException;

/**
 * Abstract Client API to submit and manage Falcon Entities (Cluster, Feed, Process) jobs
 * against an Falcon instance.
 */
public abstract class AbstractFalconClient {

    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system.
     * @param entityType
     * @param filePath
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult submit(String entityType, String filePath) throws FalconCLIException,
            IOException;

    /**
     * Schedules an submitted process entity immediately.
     * @param entityType
     * @param entityName
     * @param colo
     * @return
     * @throws FalconCLIException
     */
    public abstract APIResult schedule(EntityType entityType, String entityName,
                                       String colo, Boolean skipDryRun) throws FalconCLIException;

}
