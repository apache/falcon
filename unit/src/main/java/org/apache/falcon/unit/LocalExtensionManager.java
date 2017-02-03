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

import org.apache.commons.io.IOUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.extensions.jdbc.ExtensionMetaStore;
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.persistence.ExtensionJobsBean;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractExtensionManager;
import org.apache.falcon.resource.ExtensionJobList;
import org.apache.falcon.security.CurrentUser;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * A proxy implementation of the extension operations in local mode.
 */
public class LocalExtensionManager extends AbstractExtensionManager {
    LocalExtensionManager() {
    }

    APIResult submitExtensionJob(String extensionName, String jobName, InputStream configStream,
                                 SortedMap<EntityType, List<Entity>> entityMap) throws FalconException, IOException {
        checkIfExtensionIsEnabled(extensionName);
        checkIfExtensionJobNameExists(jobName, extensionName);
        for (Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()) {
            for (Entity entity : entry.getValue()) {
                submitInternal(entity, "falconUser");
            }
        }
        storeExtension(extensionName, jobName, configStream, entityMap);
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted successfully" + jobName);
    }

    APIResult submitAndSchedulableExtensionJob(String extensionName, String jobName, InputStream configStream,
                                               SortedMap<EntityType, List<Entity>> entityMap)
        throws FalconException, IOException {
        checkIfExtensionIsEnabled(extensionName);
        checkIfExtensionJobNameExists(jobName, extensionName);
        for (Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()) {
            for (Entity entity : entry.getValue()) {
                submitInternal(entity, "falconUser");
            }
        }

        storeExtension(extensionName, jobName, configStream, entityMap);

        for (Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()) {
            for (Entity entity : entry.getValue()) {
                scheduleInternal(entry.getKey().name(), entity.getName(), null, null);
            }
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted and scheduled successfully"
                + jobName);
    }

    private void storeExtension(String extensionName, String jobName, InputStream configStream, SortedMap<EntityType,
            List<Entity>> entityMap) throws IOException {
        byte[] configBytes = null;
        if (configStream != null) {
            configBytes = IOUtils.toByteArray(configStream);
        }
        List<String> feedNames = new ArrayList<>();
        List<String> processNames = new ArrayList<>();
        for (Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()) {
            for (final Entity entity : entry.getValue()) {
                if (entity.getEntityType().equals(EntityType.FEED)) {
                    feedNames.add(entity.getName());
                } else {
                    processNames.add(entity.getName());
                }
            }
        }
        ExtensionStore.getMetaStore().storeExtensionJob(jobName, extensionName, feedNames, processNames, configBytes);
    }

    APIResult scheduleExtensionJob(String jobName, String coloExpr, String doAsUser)
        throws FalconException, IOException {
        checkIfExtensionIsEnabled(ExtensionStore.getMetaStore().getExtensionJobDetails(jobName).getExtensionName());
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        SortedMap<EntityType, List<String>> entityMap = getJobEntities(extensionJobsBean);
        for (Map.Entry<EntityType, List<String>> entry : entityMap.entrySet()) {
            for (String entityName : entry.getValue()) {
                scheduleInternal(entry.getKey().name(), entityName, true, null);
            }
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " scheduled successfully");
    }

    APIResult deleteExtensionJob(String jobName) throws FalconException, IOException {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        SortedMap<EntityType, List<String>> entityMap = getJobEntities(extensionJobsBean);
        for (Map.Entry<EntityType, List<String>> entry : entityMap.entrySet()) {
            for (String entityName : entry.getValue()) {
                delete(entry.getKey().name(), entityName, null);
            }
        }
        ExtensionStore.getMetaStore().deleteExtensionJob(jobName);
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " deleted successfully");
    }

    APIResult updateExtensionJob(String extensionName, String jobName, InputStream configStream,
                                 SortedMap<EntityType, List<Entity>> entityMap) throws FalconException, IOException {
        List<String> feedNames = new ArrayList<>();
        List<String> processNames = new ArrayList<>();
        checkIfExtensionIsEnabled(extensionName);
        checkIfExtensionJobNameExists(jobName, extensionName);
        for (Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()) {
            for (Entity entity : entry.getValue()) {
                update(entity, entity.getEntityType().toString(), entity.getName(), true);
            }
        }
        byte[] configBytes = null;
        if (configStream != null) {
            configBytes = IOUtils.toByteArray(configStream);
        }
        for (Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()) {
            for (final Entity entity : entry.getValue()) {
                if (entity.getEntityType().equals(EntityType.FEED)) {
                    feedNames.add(entity.getName());
                } else {
                    processNames.add(entity.getName());
                }
            }
        }
        ExtensionStore.getMetaStore().updateExtensionJob(jobName, extensionName, feedNames, processNames, configBytes);
        return new APIResult(APIResult.Status.SUCCEEDED, "Updated successfully");
    }

    APIResult suspendExtensionJob(String jobName, String coloExpr, String doAsUser) throws FalconException {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        SortedMap<EntityType, List<String>> entityMap = getJobEntities(extensionJobsBean);
        for (Map.Entry<EntityType, List<String>> entityTypeEntry : entityMap.entrySet()) {
            for (String entityName : entityTypeEntry.getValue()) {
                super.suspend(null, entityTypeEntry.getKey().name(), entityName, coloExpr);
            }
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " suspended successfully");
    }

    APIResult resumeExtensionJob(String jobName, String coloExpr, String doAsUser) throws FalconException {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        SortedMap<EntityType, List<String>> entityMap = getJobEntities(extensionJobsBean);
        for (Map.Entry<EntityType, List<String>> entityTypeEntry : entityMap.entrySet()) {
            for (String entityName : entityTypeEntry.getValue()) {
                super.resume(null, entityTypeEntry.getKey().name(), entityName, coloExpr);
            }
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " suspended successfully");
    }

    APIResult registerExtensionMetadata(String extensionName, String packagePath, String description) {
        return super.registerExtensionMetadata(extensionName, packagePath, description, CurrentUser.getUser());
    }

    APIResult unRegisterExtension(String extensionName) throws FalconException {
        return super.deleteExtensionMetadata(extensionName);
    }

    APIResult getExtensionJobDetails(String jobName) {
        return super.getExtensionJobDetail(jobName);
    }

    APIResult disableExtension(String extensionName) {
        return new APIResult(APIResult.Status.SUCCEEDED, super.disableExtension(extensionName, CurrentUser.getUser()));
    }

    APIResult enableExtension(String extensionName) {
        return new APIResult(APIResult.Status.SUCCEEDED, super.enableExtension(extensionName, CurrentUser.getUser()));
    }

    APIResult getExtensionDetails(String extensionName) {
        return super.getExtensionDetail(extensionName);
    }

    public APIResult getExtensions() {
        return super.getExtensions();
    }

    public ExtensionJobList getExtensionJobs(String extensionName, String sortOrder, String doAsUser) {
        return super.getExtensionJobs(extensionName, sortOrder, doAsUser);
    }
}
