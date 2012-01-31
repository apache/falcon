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
package org.apache.ivory.mappers;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;

/**
 * Pass a partially filled coordinatorapp object project with the entity
 */
public class CoordinatorMapper implements CustomMapper {

    private final COORDINATORAPP coordinatorapp;

    private final Map<Entity, EntityType> entityMap;

    /**
     * Pass a ProcessType Object and partially filled coordinator Object
     * 
     * @param entityMap
     * @param coordinatorapp
     */
    public CoordinatorMapper(final Map<Entity, EntityType> entityMap, final COORDINATORAPP coordinatorapp) {
        super();
        this.coordinatorapp = coordinatorapp;
        this.entityMap = entityMap;
    }

    @Override
    public void mapToDefaultCoordinator() {

        for (Entry<Entity, EntityType> entityMap : this.entityMap.entrySet()) {
            if (entityMap.getValue().equals(EntityType.PROCESS)) {
                DozerProvider.map(new String[] { "process-to-coordinator.xml" }, entityMap.getKey(), this.coordinatorapp);

                // Map custom fields
                DozerProvider.map(new String[] { "custom-process-to-coordinator.xml" }, entityMap.getKey(), this.coordinatorapp);
            }
            if (entityMap.getValue().equals(EntityType.DATASET)) {
                DozerProvider.map(new String[] { "custom-default-dataset-to-coordinator.xml" }, entityMap.getKey(),
                        this.coordinatorapp);
            }
        }

    }

    @Override
    public void mapToFinalCoordinator() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<Entity, EntityType> getEntityMap() {
        return this.entityMap;
    }

    @Override
    public COORDINATORAPP getCoordinatorapp() {
        return this.coordinatorapp;
    }

}
