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

package org.apache.falcon.shell.commands;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.metadata.RelationshipType;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import static org.apache.falcon.client.FalconCLIConstants.NAME_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PIPELINE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.RELATIONS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DIRECTION_OPT;
import static org.apache.falcon.client.FalconCLIConstants.DIRECTION_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.EDGE_CMD;
import static org.apache.falcon.client.FalconCLIConstants.EDGE_CMD_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FEED_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FEED_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ID_OPT;
import static org.apache.falcon.client.FalconCLIConstants.ID_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.KEY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.KEY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LINEAGE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LINEAGE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NAME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PIPELINE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PROCESS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PROCESS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RELATIONS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VALUE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.VALUE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VERTEX_CMD;
import static org.apache.falcon.client.FalconCLIConstants.VERTEX_CMD_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VERTEX_EDGES_CMD;
import static org.apache.falcon.client.FalconCLIConstants.VERTEX_EDGES_CMD_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VERTICES_CMD;
import static org.apache.falcon.client.FalconCLIConstants.VERTICES_CMD_DESCRIPTION;
import static org.apache.falcon.ValidationUtil.validateDimensionName;
import static org.apache.falcon.ValidationUtil.validateDimensionType;
import static org.apache.falcon.ValidationUtil.validateId;
import static org.apache.falcon.ValidationUtil.validateScheduleEntity;
import static org.apache.falcon.ValidationUtil.validateVertexEdgesCommand;
import static org.apache.falcon.ValidationUtil.validateVerticesCommand;

/**
 * Metadata commands.
 */
@Component
public class FalconMetadataCommands extends BaseFalconCommands {
    public static final String METADATA_PREFIX = "metadata";
    public static final String METADATA_COMMAND_PREFIX = METADATA_PREFIX + " ";

    @CliCommand(value = {METADATA_COMMAND_PREFIX + LINEAGE_OPT}, help = LINEAGE_OPT_DESCRIPTION)
    public String lineage(
            @CliOption(key = {PIPELINE_OPT}, mandatory = true, help = PIPELINE_OPT_DESCRIPTION) final String pipeline
    ) {
        return getFalconClient().getEntityLineageGraph(pipeline, getDoAs()).getDotNotation();
    }

    @CliCommand(value = {METADATA_COMMAND_PREFIX + LIST_OPT}, help = LIST_OPT_DESCRIPTION)
    public String list(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String dimensionType,
            @CliOption(key = {CLUSTER_OPT}, mandatory = false, help = CLUSTER_OPT_DESCRIPTION) final String cluster,
            @CliOption(key = {FEED_OPT}, mandatory = false, help = FEED_OPT_DESCRIPTION) final String feed,
            @CliOption(key = {PROCESS_OPT}, mandatory = false, help = PROCESS_OPT_DESCRIPTION) final String process,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = false,
                    help = NUM_RESULTS_OPT_DESCRIPTION) final Integer numResults
    ) {
        validateDimensionType(dimensionType.toUpperCase());
        if (!(dimensionType.toUpperCase())
                .equals(RelationshipType.REPLICATION_METRICS.name())) {
            return getFalconClient().getDimensionList(dimensionType, cluster, getDoAs());
        } else {
            String schedEntityType = null;
            String schedEntityName = null;
            if (StringUtils.isNotEmpty(feed)) {
                schedEntityType = EntityType.getEnum(FEED_OPT).name();
                schedEntityName = feed;
            } else if (StringUtils.isNotEmpty(process)) {
                schedEntityType = EntityType.getEnum(PROCESS_OPT).name();
                schedEntityName = process;
            }
            validateScheduleEntity(schedEntityType, schedEntityName);

            return getFalconClient().getReplicationMetricsDimensionList(schedEntityType, schedEntityName,
                    numResults, getDoAs());
        }
    }

    @CliCommand(value = {METADATA_COMMAND_PREFIX + RELATIONS_OPT}, help = RELATIONS_OPT_DESCRIPTION)
    public String relations(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String dimensionType,
            @CliOption(key = {NAME_OPT}, mandatory = true, help = NAME_OPT_DESCRIPTION) final String dimensionName,
            @CliOption(key = {CLUSTER_OPT}, mandatory = false, help = CLUSTER_OPT_DESCRIPTION) final String cluster,
            @CliOption(key = {FEED_OPT}, mandatory = false, help = FEED_OPT_DESCRIPTION) final String feed,
            @CliOption(key = {PROCESS_OPT}, mandatory = false, help = PROCESS_OPT_DESCRIPTION) final String process,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = false,
                    help = NUM_RESULTS_OPT_DESCRIPTION) final Integer numResults
    ) {
        validateDimensionType(dimensionType.toUpperCase());
        validateDimensionName(dimensionName, RELATIONS_OPT);
        return getFalconClient().getDimensionRelations(dimensionType, dimensionName, getDoAs());
    }

    @CliCommand(value = {METADATA_COMMAND_PREFIX + VERTEX_CMD}, help = VERTEX_CMD_DESCRIPTION)
    public String vertex(
            @CliOption(key = {ID_OPT}, mandatory = true, help = ID_OPT_DESCRIPTION) final String id
    ) {
        validateId(id);
        return getFalconClient().getVertex(id, getDoAs());
    }
    @CliCommand(value = {METADATA_COMMAND_PREFIX + EDGE_CMD}, help = EDGE_CMD_DESCRIPTION)
    public String edge(
            @CliOption(key = {ID_OPT}, mandatory = true, help = ID_OPT_DESCRIPTION) final String id
    ) {
        validateId(id);
        return getFalconClient().getEdge(id, getDoAs());
    }
    @CliCommand(value = {METADATA_COMMAND_PREFIX + VERTICES_CMD}, help = VERTICES_CMD_DESCRIPTION)
    public String vertices(
            @CliOption(key = {KEY_OPT}, mandatory = true, help = KEY_OPT_DESCRIPTION) final String key,
            @CliOption(key = {VALUE_OPT}, mandatory = true, help = VALUE_OPT_DESCRIPTION) final String value
    ) {
        validateVerticesCommand(key, value);
        return getFalconClient().getVertices(key, value, getDoAs());
    }
    @CliCommand(value = {METADATA_COMMAND_PREFIX + VERTEX_EDGES_CMD}, help = VERTEX_EDGES_CMD_DESCRIPTION)
    public String vertexEdges(
            @CliOption(key = {ID_OPT}, mandatory = true, help = ID_OPT_DESCRIPTION) final String id,
            @CliOption(key = {DIRECTION_OPT}, mandatory = true, help = DIRECTION_OPT_DESCRIPTION) final String direction
    ) {
        validateVertexEdgesCommand(id, direction);
        return getFalconClient().getVertexEdges(id, direction, getDoAs());
    }

}
