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
import org.apache.falcon.client.FalconCLIConstants;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.metadata.RelationshipType;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.falcon.client.FalconCLIConstants.DISCOVERY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.DISCOVERY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LINEAGE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LINEAGE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PIPELINE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PIPELINE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RELATIONS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.RELATIONS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.URL_OPTION;
import static org.apache.falcon.client.FalconCLIConstants.URL_OPTION_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NAME_OPT;
import static org.apache.falcon.client.FalconCLIConstants.NAME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEBUG_OPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEBUG_OPTION_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DIRECTION_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DIRECTION_OPT;
import static org.apache.falcon.client.FalconCLIConstants.VALUE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VALUE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.KEY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.KEY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.ID_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ID_OPT;
import static org.apache.falcon.client.FalconCLIConstants.EDGE_CMD_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.EDGE_CMD;
import static org.apache.falcon.client.FalconCLIConstants.VERTEX_EDGES_CMD_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VERTEX_EDGES_CMD;
import static org.apache.falcon.client.FalconCLIConstants.VERTICES_CMD_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VERTICES_CMD;
import static org.apache.falcon.client.FalconCLIConstants.VERTEX_CMD_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VERTEX_CMD;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PROCESS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PROCESS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FEED_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FEED_OPT;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.DO_AS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.DO_AS_DESCRIPTION;
import static org.apache.falcon.ValidationUtil.validateDimensionName;
import static org.apache.falcon.ValidationUtil.validateDimensionType;
import static org.apache.falcon.ValidationUtil.validateId;
import static org.apache.falcon.ValidationUtil.validateScheduleEntity;
import static org.apache.falcon.ValidationUtil.validateVertexEdgesCommand;
import static org.apache.falcon.ValidationUtil.validateVerticesCommand;
import static org.apache.falcon.ValidationUtil.validatePipelineName;



/**
 * Metadata extension to Falcon Command Line Interface - wraps the RESTful API for Metadata.
 */
public class FalconMetadataCLI extends FalconCLI {

    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);


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

        Option doAs = new Option(DO_AS_OPT, true, DO_AS_DESCRIPTION);

        metadataOptions.addOption(doAs);

        return metadataOptions;
    }

    public void metadataCommand(CommandLine commandLine, FalconClient client) {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String dimensionType = commandLine.getOptionValue(FalconCLIConstants.TYPE_OPT);
        String cluster = commandLine.getOptionValue(FalconCLIConstants.CLUSTER_OPT);
        String feed = commandLine.getOptionValue(FalconCLIConstants.FEED_OPT);
        String process = commandLine.getOptionValue(FalconCLIConstants.PROCESS_OPT);
        String dimensionName = commandLine.getOptionValue(FalconCLIConstants.NAME_OPT);
        String id = commandLine.getOptionValue(ID_OPT);
        String key = commandLine.getOptionValue(KEY_OPT);
        String value = commandLine.getOptionValue(VALUE_OPT);
        String direction = commandLine.getOptionValue(DIRECTION_OPT);
        String pipeline = commandLine.getOptionValue(FalconCLIConstants.PIPELINE_OPT);
        String doAsUser = commandLine.getOptionValue(FalconCLIConstants.DO_AS_OPT);
        Integer numResults = parseIntegerInput(commandLine.getOptionValue(FalconCLIConstants.NUM_RESULTS_OPT),
                null, "numResults");

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
                    schedEntityType = EntityType.getEnum(FalconCLIConstants.FEED_OPT).name();
                    schedEntityName = feed;
                } else if (StringUtils.isNotEmpty(process)) {
                    schedEntityType = EntityType.getEnum(FalconCLIConstants.PROCESS_OPT).name();
                    schedEntityName = process;
                }
                validateScheduleEntity(schedEntityType, schedEntityName);

                result = client.getReplicationMetricsDimensionList(schedEntityType, schedEntityName,
                        numResults, doAsUser);
            }
        } else if (optionsList.contains(FalconCLIConstants.RELATIONS_OPT)) {
            validateDimensionType(dimensionType.toUpperCase());
            validateDimensionName(dimensionName, FalconCLIConstants.RELATIONS_OPT);
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
            throw new FalconCLIException("Invalid/missing metadata command. Supported commands include "
                    + "list, relations, lineage, vertex, vertices, edge, edges. "
                    + "Please refer to Falcon CLI twiki for more details.");
        }

        OUT.get().println(result);
    }


}
