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

package org.apache.falcon.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.metadata.RelationshipType;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Metadata extension to Falcon Command Line Interface - wraps the RESTful API for Metadata.
 */
public class FalconMetadataCLI extends FalconCLI {

    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    // Discovery Commands
    public static final String DISCOVERY_OPT = "discovery";
    public static final String LIST_OPT = "list";
    public static final String RELATIONS_OPT = "relations";
    public static final String URL_OPTION = "url";
    public static final String NAME_OPT = "name";

    // Lineage Commands
    public static final String LINEAGE_OPT = "lineage";
    public static final String VERTEX_CMD = "vertex";
    public static final String VERTICES_CMD = "vertices";
    public static final String VERTEX_EDGES_CMD = "edges";
    public static final String PIPELINE_OPT = "pipeline";
    public static final String EDGE_CMD = "edge";
    public static final String ID_OPT = "id";
    public static final String KEY_OPT = "key";
    public static final String VALUE_OPT = "value";
    public static final String DIRECTION_OPT = "direction";

    public static final String DISCOVERY_OPT_DESCRIPTION = "Discover falcon metadata relations";
    public static final String LINEAGE_OPT_DESCRIPTION = "Get falcon metadata lineage information";
    public static final String PIPELINE_OPT_DESCRIPTION = "Get lineage graph for the entities in a pipeline";
    public static final String LIST_OPT_DESCRIPTION = "List all dimensions";
    public static final String RELATIONS_OPT_DESCRIPTION = "List all relations for a dimension";
    public static final String URL_OPTION_DESCRIPTION = "Falcon URL";
    public static final String TYPE_OPT_DESCRIPTION = "Dimension type";
    public static final String NAME_OPT_DESCRIPTION = "Dimension name";
    public static final String CLUSTER_OPT_DESCRIPTION = "Cluster name";
    public static final String FEED_OPT_DESCRIPTION = "Feed Entity name";
    public static final String PROCESS_OPT_DESCRIPTION = "Process Entity name";
    public static final String NUM_RESULTS_OPT_DESCRIPTION = "Number of results to return per request";
    public static final String VERTEX_CMD_DESCRIPTION = "show the vertices";
    public static final String VERTICES_CMD_DESCRIPTION = "show the vertices";
    public static final String VERTEX_EDGES_CMD_DESCRIPTION = "show the edges for a given vertex";
    public static final String EDGE_CMD_DESCRIPTION = "show the edges";
    public static final String ID_OPT_DESCRIPTION = "vertex or edge id";
    public static final String KEY_OPT_DESCRIPTION = "key property";
    public static final String VALUE_OPT_DESCRIPTION = "value property";
    public static final String DIRECTION_OPT_DESCRIPTION = "edge direction property";
    public static final String DEBUG_OPTION_DESCRIPTION = "Use debug mode to see debugging statements on stdout";
    public static final String FalconCLI_DESCRIPTION = "doAs user";

    public FalconMetadataCLI() throws Exception {
        super();
    }

    public Options createMetadataOptions() {
        Options metadataOptions = new Options();

        OptionGroup group = new OptionGroup();
        Option discovery = new Option(DISCOVERY_OPT, false, DISCOVERY_OPT_DESCRIPTION);
        Option lineage = new Option(LINEAGE_OPT, false, LINEAGE_OPT_DESCRIPTION);
        group.addOption(discovery);
        group.addOption(lineage);
        Option pipeline = new Option(PIPELINE_OPT, true, PIPELINE_OPT_DESCRIPTION);
        metadataOptions.addOptionGroup(group);

        // Add discovery options

        Option list = new Option(LIST_OPT, false, LIST_OPT_DESCRIPTION);
        Option relations = new Option(RELATIONS_OPT, false, RELATIONS_OPT_DESCRIPTION);
        metadataOptions.addOption(list);
        metadataOptions.addOption(relations);

        Option url = new Option(URL_OPTION, true, URL_OPTION_DESCRIPTION);
        Option type = new Option(TYPE_OPT, true, TYPE_OPT_DESCRIPTION);
        Option name = new Option(NAME_OPT, true, NAME_OPT_DESCRIPTION);
        Option cluster = new Option(CLUSTER_OPT, true, CLUSTER_OPT_DESCRIPTION);
        Option feed = new Option(FEED_OPT, true, FEED_OPT_DESCRIPTION);
        Option process = new Option(PROCESS_OPT, true, PROCESS_OPT_DESCRIPTION);
        Option numResults = new Option(NUM_RESULTS_OPT, true, NUM_RESULTS_OPT_DESCRIPTION);

        // Add lineage options
        metadataOptions.addOption(pipeline);

        metadataOptions.addOption(url);
        metadataOptions.addOption(type);
        metadataOptions.addOption(cluster);
        metadataOptions.addOption(name);
        metadataOptions.addOption(feed);
        metadataOptions.addOption(process);
        metadataOptions.addOption(numResults);

        Option vertex = new Option(VERTEX_CMD, false, VERTEX_CMD_DESCRIPTION);
        Option vertices = new Option(VERTICES_CMD, false, VERTICES_CMD_DESCRIPTION);
        Option vertexEdges = new Option(VERTEX_EDGES_CMD, false, VERTEX_EDGES_CMD_DESCRIPTION);
        Option edges = new Option(EDGE_CMD, false, EDGE_CMD_DESCRIPTION);
        Option id = new Option(ID_OPT, true, ID_OPT_DESCRIPTION);
        Option key = new Option(KEY_OPT, true, KEY_OPT_DESCRIPTION);
        Option value = new Option(VALUE_OPT, true, VALUE_OPT_DESCRIPTION);
        Option direction = new Option(DIRECTION_OPT, true, DIRECTION_OPT_DESCRIPTION);
        Option debug = new Option(DEBUG_OPTION, false, DEBUG_OPTION_DESCRIPTION);

        metadataOptions.addOption(vertex);
        metadataOptions.addOption(vertices);
        metadataOptions.addOption(vertexEdges);
        metadataOptions.addOption(edges);
        metadataOptions.addOption(id);
        metadataOptions.addOption(key);
        metadataOptions.addOption(value);
        metadataOptions.addOption(direction);
        metadataOptions.addOption(debug);

        Option doAs = new Option(FalconCLI.DO_AS_OPT, true, FalconCLI_DESCRIPTION);
        metadataOptions.addOption(doAs);

        return metadataOptions;
    }

    public void metadataCommand(CommandLine commandLine, FalconClient client) {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String dimensionType = commandLine.getOptionValue(TYPE_OPT);
        String cluster = commandLine.getOptionValue(CLUSTER_OPT);
        String feed = commandLine.getOptionValue(FEED_OPT);
        String process = commandLine.getOptionValue(PROCESS_OPT);
        String dimensionName = commandLine.getOptionValue(NAME_OPT);
        String id = commandLine.getOptionValue(ID_OPT);
        String key = commandLine.getOptionValue(KEY_OPT);
        String value = commandLine.getOptionValue(VALUE_OPT);
        String direction = commandLine.getOptionValue(DIRECTION_OPT);
        String pipeline = commandLine.getOptionValue(PIPELINE_OPT);
        String doAsUser = commandLine.getOptionValue(FalconCLI.DO_AS_OPT);
        Integer numResults = parseIntegerInput(commandLine.getOptionValue(NUM_RESULTS_OPT), null, "numResults");

        if (optionsList.contains(LINEAGE_OPT)) {
            validatePipelineName(pipeline);
            result = client.getEntityLineageGraph(pipeline, doAsUser).getDotNotation();
        } else if (optionsList.contains(LIST_OPT)) {
            validateDimensionType(dimensionType.toUpperCase());
            if (!(dimensionType.toUpperCase())
                    .equals(RelationshipType.REPLICATION_METRICS.name())) {
                result = client.getDimensionList(dimensionType, cluster, doAsUser);
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

                result = client.getReplicationMetricsDimensionList(schedEntityType, schedEntityName,
                        numResults, doAsUser);
            }
        } else if (optionsList.contains(RELATIONS_OPT)) {
            validateDimensionType(dimensionType.toUpperCase());
            validateDimensionName(dimensionName, RELATIONS_OPT);
            result = client.getDimensionRelations(dimensionType, dimensionName, doAsUser);
        } else if (optionsList.contains(VERTEX_CMD)) {
            validateId(id);
            result = client.getVertex(id, doAsUser);
        } else if (optionsList.contains(VERTICES_CMD)) {
            validateVerticesCommand(key, value);
            result = client.getVertices(key, value, doAsUser);
        } else if (optionsList.contains(VERTEX_EDGES_CMD)) {
            validateVertexEdgesCommand(id, direction);
            result = client.getVertexEdges(id, direction, doAsUser);
        } else if (optionsList.contains(EDGE_CMD)) {
            validateId(id);
            result = client.getEdge(id, doAsUser);
        } else {
            throw new FalconCLIException("Invalid metadata command");
        }

        OUT.get().println(result);
    }

    public static void validatePipelineName(String pipeline) {
        if (StringUtils.isEmpty(pipeline)) {
            throw new FalconCLIException("Invalid value for pipeline");
        }
    }

    public static void validateDimensionType(String dimensionType) {
        if (StringUtils.isEmpty(dimensionType)
                ||  dimensionType.contains("INSTANCE")) {
            throw new FalconCLIException("Invalid value provided for queryParam \"type\" " + dimensionType);
        }
        try {
            RelationshipType.valueOf(dimensionType);
        } catch (IllegalArgumentException iae) {
            throw new FalconCLIException("Invalid value provided for queryParam \"type\" " + dimensionType);
        }
    }

    public static void validateDimensionName(String dimensionName, String action) {
        if (StringUtils.isEmpty(dimensionName)) {
            throw new FalconCLIException("Dimension ID cannot be empty or null for action " + action);
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

    public static void validateId(String id) {
        if (id == null || id.length() == 0) {
            throw new FalconCLIException("Missing argument: id");
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

    public static void validateVertexEdgesCommand(String id, String direction) {
        if (id == null || id.length() == 0) {
            throw new FalconCLIException("Missing argument: id");
        }

        if (direction == null || direction.length() == 0) {
            throw new FalconCLIException("Missing argument: direction");
        }
    }
}
