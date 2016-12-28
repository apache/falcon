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
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractExtensionManager;
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
    public LocalExtensionManager() {}

    public APIResult submitExtensionJob(String extensionName, String jobName, InputStream config,
                                        SortedMap<EntityType, List<Entity>> entityMap)
        throws FalconException, IOException {

        for(Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()){
            for(Entity entity : entry.getValue()){
                submitInternal(entity, "falconUser");
            }
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted successfully" + jobName);
    }

    public APIResult submitAndSchedulableExtensionJob(String extensionName, String jobName, InputStream configStream,
                                                      SortedMap<EntityType, List<Entity>> entityMap)
        throws FalconException, IOException {
        List<String> feedNames = new ArrayList<>();
        List<String> processNames = new ArrayList<>();
        for(Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()){
            for(Entity entity : entry.getValue()){
                submitInternal(entity, "falconUser");
            }
        }

        for(Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()){
            for(Entity entity : entry.getValue()){
                scheduleInternal(entry.getKey().name(), entity.getName(), null, null);
            }
        }
        byte[] configBytes = null;
        if (configStream != null) {
            configBytes = IOUtils.toByteArray(configStream);
        }
        for(Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()){
            for(final Entity entity : entry.getValue()){
                if (entity.getEntityType().equals(EntityType.FEED)){
                    feedNames.add(entity.getName());
                }else{
                    processNames.add(entity.getName());
                }
            }
        }
        ExtensionStore.getMetaStore().storeExtensionJob(jobName, extensionName, feedNames, processNames, configBytes);

        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted successfully" + jobName);
    }

    public APIResult updateExtensionJob(String extensionName, String jobName, InputStream configStream,
                                        SortedMap<EntityType, List<Entity>> entityMap)
        throws FalconException, IOException {
        List<String> feedNames = new ArrayList<>();
        List<String> processNames = new ArrayList<>();
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

    public APIResult registerExtensionMetadata(String extensionName, String packagePath , String description) {
        return super.registerExtensionMetadata(extensionName, packagePath, description, CurrentUser.getUser());
    }

    public APIResult unRegisterExtension(String extensionName) {
        return super.deleteExtensionMetadata(extensionName);
    }

    public APIResult getExtensionJobDetails(String jobName){
        return super.getExtensionJobDetail(jobName);
    }

    public APIResult getExtensionDetails(String extensionName){
        return super.getExtensionDetail(extensionName);
    }

    public APIResult getExtensions(){
        return super.getExtensions();
    }

}
