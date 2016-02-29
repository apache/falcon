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

package org.apache.falcon.cli.commands;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.metadata.RelationshipType;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;


import static org.apache.falcon.cli.FalconMetadataCLI.*;

/**
 * Instance commands.
 */
@Component
public class FalconMetadataCommands extends BaseFalconCommands {
    public static final String METADATA_PREFIX = "metadata";
    public static final String METADATA_COMMAND_PREFIX = METADATA_PREFIX + " ";

    @CliCommand(value = {METADATA_COMMAND_PREFIX + LINEAGE_OPT}, help = LINEAGE_OPT_DESCRIPTION)
    public String lineage(
            @CliOption(key = {PIPELINE_OPT}, mandatory = true, help = PIPELINE_OPT_DESCRIPTION) final String pipeline
    ) {
        return getFalconClient().getEntityLineageGraph(pipeline, getDoAs());
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
