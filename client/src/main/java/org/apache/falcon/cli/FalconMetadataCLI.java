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
import org.apache.falcon.metadata.RelationshipType;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Metadata extension to Falcon Command Line Interface - wraps the RESTful API for Metadata.
 */
public class FalconMetadataCLI {

    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    // Discovery Commands
    public static final String DISCOVERY_OPT = "discovery";
    public static final String LIST_OPT = "list";
    public static final String RELATIONS_OPT = "relations";

    public static final String URL_OPTION = "url";
    public static final String TYPE_OPT = "type";
    public static final String CLUSTER_OPT = "cluster";
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


    public FalconMetadataCLI() {}

    public void metadataCommand(CommandLine commandLine, FalconClient client) throws FalconCLIException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String dimensionType = commandLine.getOptionValue(TYPE_OPT);
        String cluster = commandLine.getOptionValue(CLUSTER_OPT);
        String dimensionName = commandLine.getOptionValue(NAME_OPT);
        String id = commandLine.getOptionValue(ID_OPT);
        String key = commandLine.getOptionValue(KEY_OPT);
        String value = commandLine.getOptionValue(VALUE_OPT);
        String direction = commandLine.getOptionValue(DIRECTION_OPT);
        String pipeline = commandLine.getOptionValue(PIPELINE_OPT);

        if (optionsList.contains(LINEAGE_OPT)) {
            validatePipelineName(pipeline);
            result = client.getEntityLineageGraph(pipeline).getDotNotation();
        } else if (optionsList.contains(LIST_OPT)) {
            validateDimensionType(dimensionType.toUpperCase());
            result = client.getDimensionList(dimensionType, cluster);
        } else if (optionsList.contains(RELATIONS_OPT)) {
            validateDimensionType(dimensionType.toUpperCase());
            validateDimensionName(dimensionName, RELATIONS_OPT);
            result = client.getDimensionRelations(dimensionType, dimensionName);
        } else if (optionsList.contains(VERTEX_CMD)) {
            validateId(id);
            result = client.getVertex(id);
        } else if (optionsList.contains(VERTICES_CMD)) {
            validateVerticesCommand(key, value);
            result = client.getVertices(key, value);
        } else if (optionsList.contains(VERTEX_EDGES_CMD)) {
            validateVertexEdgesCommand(id, direction);
            result = client.getVertexEdges(id, direction);
        } else if (optionsList.contains(EDGE_CMD)) {
            validateId(id);
            result = client.getEdge(id);
        } else {
            throw new FalconCLIException("Invalid metadata command");
        }

        OUT.get().println(result);
    }

    private void validatePipelineName(String pipeline) throws FalconCLIException {
        if (StringUtils.isEmpty(pipeline)) {
            throw new FalconCLIException("Invalid value for pipeline");
        }
    }

    private void validateDimensionType(String dimensionType) throws FalconCLIException {
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

    private void validateDimensionName(String dimensionName, String action) throws FalconCLIException {
        if (StringUtils.isEmpty(dimensionName)) {
            throw new FalconCLIException("Dimension ID cannot be empty or null for action " + action);
        }
    }

    private void validateId(String id) throws FalconCLIException {
        if (id == null || id.length() == 0) {
            throw new FalconCLIException("Missing argument: id");
        }
    }

    private void validateVerticesCommand(String key, String value) throws FalconCLIException {
        if (key == null || key.length() == 0) {
            throw new FalconCLIException("Missing argument: key");
        }

        if (value == null || value.length() == 0) {
            throw new FalconCLIException("Missing argument: value");
        }
    }

    private void validateVertexEdgesCommand(String id, String direction) throws FalconCLIException {
        if (id == null || id.length() == 0) {
            throw new FalconCLIException("Missing argument: id");
        }

        if (direction == null || direction.length() == 0) {
            throw new FalconCLIException("Missing argument: direction");
        }
    }

    public Options createMetadataOptions() {
        Options metadataOptions = new Options();

        OptionGroup group = new OptionGroup();
        Option discovery = new Option(DISCOVERY_OPT, false, "Discover falcon metadata relations");
        Option lineage = new Option(LINEAGE_OPT, false, "Get falcon metadata lineage information");
        group.addOption(discovery);
        group.addOption(lineage);
        Option pipeline = new Option(PIPELINE_OPT, true,
                "Get lineage graph for the entities in a pipeline");
        metadataOptions.addOptionGroup(group);

        // Add discovery options

        Option list = new Option(LIST_OPT, false, "List all dimensions");
        Option relations = new Option(RELATIONS_OPT, false, "List all relations for a dimension");
        metadataOptions.addOption(list);
        metadataOptions.addOption(relations);

        Option url = new Option(URL_OPTION, true, "Falcon URL");
        Option type = new Option(TYPE_OPT, true, "Dimension type");
        Option name = new Option(NAME_OPT, true, "Dimension name");
        Option cluster = new Option(CLUSTER_OPT, true, "Cluster name");

        // Add lineage options
        metadataOptions.addOption(pipeline);

        metadataOptions.addOption(url);
        metadataOptions.addOption(type);
        metadataOptions.addOption(cluster);
        metadataOptions.addOption(name);

        Option vertex = new Option(VERTEX_CMD, false, "show the vertices");
        Option vertices = new Option(VERTICES_CMD, false, "show the vertices");
        Option vertexEdges = new Option(VERTEX_EDGES_CMD, false, "show the edges for a given vertex");
        Option edges = new Option(EDGE_CMD, false, "show the edges");
        Option id = new Option(ID_OPT, true, "vertex or edge id");
        Option key = new Option(KEY_OPT, true, "key property");
        Option value = new Option(VALUE_OPT, true, "value property");
        Option direction = new Option(DIRECTION_OPT, true, "edge direction property");

        metadataOptions.addOption(vertex);
        metadataOptions.addOption(vertices);
        metadataOptions.addOption(vertexEdges);
        metadataOptions.addOption(edges);
        metadataOptions.addOption(id);
        metadataOptions.addOption(key);
        metadataOptions.addOption(value);
        metadataOptions.addOption(direction);

        return metadataOptions;
    }
}
