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
import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.UnschedulableEntityException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for Falcon Unit.
 */
public final class FalconUnitHelper {
    private FalconUnitHelper() {
    }

    /**
     * Converts a InputStream into FileInputStream.
     *
     * @param filePath - Path of file to stream
     * @return ServletInputStream
     * @throws org.apache.falcon.FalconException
     */
    public static InputStream getFileInputStream(String filePath) throws FalconException {
        if (filePath == null) {
            throw new IllegalArgumentException("file path should not be null");
        }
        InputStream stream;
        try {
            stream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new FalconException("File not found: " + filePath);
        }
        return stream;
    }

    /**
     * Updates lifecycle based on entity.
     *
     * @param lifeCycleValues
     * @param type            entity type
     * @return list of lifecycle values after check and update
     */
    public static List<LifeCycle> checkAndUpdateLifeCycle(List<LifeCycle> lifeCycleValues,
                                                          String type) throws FalconException {
        EntityType entityType = EntityType.getEnum(type);
        if (lifeCycleValues == null || lifeCycleValues.isEmpty()) {
            List<LifeCycle> lifeCycles = new ArrayList<LifeCycle>();
            if (entityType == EntityType.PROCESS) {
                lifeCycles.add(LifeCycle.valueOf(LifeCycle.EXECUTION.name()));
            } else if (entityType == EntityType.FEED) {
                lifeCycles.add(LifeCycle.valueOf(LifeCycle.REPLICATION.name()));
            }
            return lifeCycles;
        }
        for (LifeCycle lifeCycle : lifeCycleValues) {
            if (entityType != lifeCycle.getTag().getType()) {
                throw new FalconException("Incorrect lifecycle: " + lifeCycle + "for given type: " + type);
            }
        }
        return lifeCycleValues;
    }

    /**
     * Checks entity is schedulable or not.
     *
     * @param type
     * @throws UnschedulableEntityException
     */
    public static void checkSchedulableEntity(String type) throws UnschedulableEntityException {
        EntityType entityType = EntityType.getEnum(type);
        if (!entityType.isSchedulable()) {
            throw new UnschedulableEntityException(
                    "Entity type (" + type + ") " + " cannot be Scheduled/Suspended/Resumed");
        }
    }
}
