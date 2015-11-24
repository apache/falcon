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
package org.apache.falcon.unit;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.hadoop.security.authorize.AuthorizationException;

import java.io.IOException;
import java.io.InputStream;

/**
 * A proxy implementation of the schedulable entity operations in local mode.
 */
public class LocalSchedulableEntityManager extends AbstractSchedulableEntityManager {

    public LocalSchedulableEntityManager() {}

    public APIResult suspend(String type, String entity, String colo) {
        return super.suspend(null, type, entity, colo);
    }

    public APIResult resume(String type, String entity, String colo) {
        return super.resume(null, type, entity, colo);
    }

    public APIResult getStatus(String type, String entity, String colo) {
        return super.getStatus(type, entity, colo);
    }

    public APIResult delete(EntityType entityType, String entityName, String doAsUser) {
        if (entityType == null) {
            throw new IllegalStateException("Entity-Type cannot be null");
        }
        return super.delete(entityType.name(), entityName, doAsUser);
    }

    public APIResult validate(String entityType, String filePath, Boolean skipDryRun,
                              String doAsUser) throws FalconException {
        InputStream inputStream = FalconUnitHelper.getFileInputStream(filePath);
        return super.validate(inputStream, entityType, skipDryRun);
    }

    public APIResult update(String entityType, String entityName, String filePath,
                            Boolean skipDryRun, String doAsUser, String colo) throws FalconException {
        InputStream inputStream = FalconUnitHelper.getFileInputStream(filePath);
        return super.update(inputStream, entityType, entityName, colo, skipDryRun);
    }

    public APIResult submit(String entityType, String filePath, String doAsUser) throws FalconException, IOException {
        InputStream inputStream = FalconUnitHelper.getFileInputStream(filePath);
        Entity entity = super.submitInternal(inputStream, entityType, doAsUser);
        return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + entityType + ") " + entity.getName());
    }

    public APIResult schedule(EntityType entityType, String entityName, Boolean skipDryRun, String properties) throws
            FalconException, AuthorizationException {
        scheduleInternal(entityType.name(), entityName, skipDryRun,  EntityUtil.getPropertyMap(properties));
        return new APIResult(APIResult.Status.SUCCEEDED, entityName + "(" + entityType + ") scheduled successfully");
    }

    public APIResult submitAndSchedule(String entityType, String filePath, Boolean skipDryRun, String doAsUser,
                                       String properties) throws FalconException, IOException {
        InputStream inputStream = FalconUnitHelper.getFileInputStream(filePath);
        Entity entity = super.submitInternal(inputStream, entityType, doAsUser);
        scheduleInternal(entityType, entity.getName(), skipDryRun, EntityUtil.getPropertyMap(properties));
        return new APIResult(APIResult.Status.SUCCEEDED,
                entity.getName() + "(" + entityType + ") scheduled successfully");
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public EntityList getEntityList(String fieldStr, String nameSubsequence, String tagKeywords,
                                    String filterType, String filterTags, String filterBy,
                                    String orderBy, String sortOrder, Integer offset,
                                    Integer resultsPerPage, final String doAsUser) {
        return super.getEntityList(fieldStr, nameSubsequence, tagKeywords, filterType, filterTags, filterBy, orderBy,
                sortOrder, offset, resultsPerPage, doAsUser);
    }

    public EntitySummaryResult getEntitySummary(String type, String cluster, String startDate, String endDate,
                                                String fields, String filterBy, String filterTags,
                                                String orderBy, String sortOrder, Integer offset,
                                                Integer resultsPerPage, Integer numInstances, final String doAsUser) {
        return super.getEntitySummary(type, cluster, startDate, endDate, fields, filterBy, filterTags, orderBy,
                sortOrder, offset, resultsPerPage, numInstances, doAsUser);
    }

    public APIResult touch(String type, String entityName, String colo, Boolean skipDryRun) {
        return super.touch(type, entityName, colo, skipDryRun);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

}
