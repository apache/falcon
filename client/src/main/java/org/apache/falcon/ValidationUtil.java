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

package org.apache.falcon;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.client.FalconCLIConstants;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.metadata.RelationshipType;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * All the validation methods to check the params from CLI and Shell.
 */
public final class ValidationUtil {

    private ValidationUtil(){}

    public static void validateEntityFields(String fields) {
        if (StringUtils.isEmpty(fields)) {
            return;
        }
        String[] fieldsList = fields.split(",");
        for (String s : fieldsList) {
            try {
                EntityList.EntityFieldList.valueOf(s.toUpperCase());
            } catch (IllegalArgumentException ie) {
                throw new FalconCLIException("Invalid fields argument : " + FalconCLIConstants.FIELDS_OPT);
            }
        }
    }

    public static void validateOrderBy(String orderBy, String action) {
        if (StringUtils.isBlank(orderBy)) {
            return;
        }
        if (action.equals("instance")) {
            if (Arrays.asList(new String[]{"status", "cluster", "starttime", "endtime"})
                    .contains(orderBy.toLowerCase())) {
                return;
            }
        } else if (action.equals("entity")) {
            if (Arrays.asList(new String[] {"type", "name"}).contains(orderBy.toLowerCase())) {
                return;
            }
        } else if (action.equals("summary")) {
            if (Arrays.asList(new String[]{"cluster"})
                    .contains(orderBy.toLowerCase())) {
                return;
            }
        }
        throw new FalconCLIException("Invalid orderBy argument : " + orderBy);
    }

    public static void validateFilterBy(String filterBy, String filterType) {
        if (StringUtils.isBlank(filterBy)) {
            return;
        }
        String[] filterSplits = filterBy.split(",");
        for (String s : filterSplits) {
            String[] tempKeyVal = s.split(":", 2);
            try {
                if (filterType.equals("entity")) {
                    EntityList.EntityFilterByFields.valueOf(tempKeyVal[0].toUpperCase());
                } else if (filterType.equals("instance")) {
                    InstancesResult.InstanceFilterFields.valueOf(tempKeyVal[0].toUpperCase());
                }else if (filterType.equals("summary")) {
                    InstancesSummaryResult.InstanceSummaryFilterFields.valueOf(tempKeyVal[0].toUpperCase());
                } else {
                    throw new IllegalArgumentException("Invalid API call: filterType is not valid");
                }
            } catch (IllegalArgumentException ie) {
                throw new FalconCLIException("Invalid filterBy argument : " + tempKeyVal[0] + " in : " + s);
            }
        }
    }

    public static void validateEntityTypeForSummary(String type) {
        EntityType entityType = EntityType.getEnum(type);
        if (!entityType.isSchedulable()) {
            throw new FalconCLIException("Invalid entity type " + entityType
                    + " for EntitySummary API. Valid options are feed or process");
        }
    }

    public static List<LifeCycle> getLifeCycle(String lifeCycleValue) {
        if (lifeCycleValue != null) {
            String[] lifeCycleValues = lifeCycleValue.split(",");
            List<LifeCycle> lifeCycles = new ArrayList<>();
            try {
                for (String lifeCycle : lifeCycleValues) {
                    lifeCycles.add(LifeCycle.valueOf(lifeCycle.toUpperCase().trim()));
                }
            } catch (IllegalArgumentException e) {
                throw new FalconCLIException("Invalid life cycle values: " + lifeCycles, e);
            }
            return lifeCycles;
        }
        return null;
    }

    public static void validateDimensionName(String dimensionName, String action) {
        if (StringUtils.isBlank(dimensionName)) {
            throw new FalconCLIException("Dimension ID cannot be empty or null for action " + action);
        }
    }

    public static void validateDimensionType(String dimensionType) {
        if (StringUtils.isBlank(dimensionType)
                ||  dimensionType.contains("INSTANCE")) {
            throw new FalconCLIException("Invalid value provided for queryParam \"type\" " + dimensionType);
        }
        try {
            RelationshipType.valueOf(dimensionType);
        } catch (IllegalArgumentException iae) {
            throw new FalconCLIException("Invalid value provided for queryParam \"type\" " + dimensionType);
        }
    }

    public static void validateId(String id) {
        if (id == null || id.length() == 0) {
            throw new FalconCLIException("Missing argument: id");
        }
    }

    public static void validateScheduleEntity(String schedEntityType, String schedEntityName) {
        if (StringUtils.isBlank(schedEntityType)) {
            throw new FalconCLIException("Entity must be schedulable type : -feed/process");
        }

        if (StringUtils.isBlank(schedEntityName)) {
            throw new FalconCLIException("Entity name is missing");
        }
    }

    public static void validateVertexEdgesCommand(String id, String direction) {
        if (id == null || id.length() == 0) {
            throw new FalconCLIException("Missing argument: id");
        }

        if (direction == null || direction.length() == 0) {
            throw new FalconCLIException("Missing argument: direction");
        }
    }

    public static void validateVerticesCommand(String key, String value) {
        if (key == null || key.length() == 0) {
            throw new FalconCLIException("Missing argument: key");
        }

        if (value == null || value.length() == 0) {
            throw new FalconCLIException("Missing argument: value");
        }
    }

    public static void validatePipelineName(String pipeline) {
        if (StringUtils.isBlank(pipeline)) {
            throw new FalconCLIException("Invalid value for pipeline");
        }
    }

    public static void validateNotEmpty(String paramVal, String paramName) {
        if (StringUtils.isBlank(paramVal)) {
            throw new FalconCLIException("Missing argument : " + paramName);
        }
    }

    public static void validateSortOrder(String sortOrder) {
        if (!StringUtils.isBlank(sortOrder)) {
            if (!sortOrder.equalsIgnoreCase("asc") && !sortOrder.equalsIgnoreCase("desc")) {
                throw new FalconCLIException("Value for param sortOrder should be \"asc\" or \"desc\". It is  : "
                        + sortOrder);
            }
        }
    }

}
