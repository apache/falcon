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
import org.apache.commons.lang.StringUtils;
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

    public static final String LIST_OPT = "list";

    public static final String URL_OPTION = "url";
    public static final String TYPE_OPT = "type";
    public static final String CLUSTER_OPT = "cluster";

    public FalconMetadataCLI() {}

    public void metadataCommand(CommandLine commandLine, FalconClient client) throws FalconCLIException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result = null;
        String dimensionType = commandLine.getOptionValue(TYPE_OPT);
        String cluster = commandLine.getOptionValue(CLUSTER_OPT);

        validateDimensionType(dimensionType.toUpperCase());

        if (optionsList.contains(LIST_OPT)) {
            result = client.getDimensionList(dimensionType, cluster);
        }

        OUT.get().println(result);
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

    public Options createMetadataOptions() {
        Options metadataOptions = new Options();

        OptionGroup group = new OptionGroup();
        Option list = new Option(LIST_OPT, false, "List all dimensions");
        group.addOption(list);

        Option url = new Option(URL_OPTION, true, "Falcon URL");
        Option type = new Option(TYPE_OPT, true, "Dimension type");
        Option cluster = new Option(CLUSTER_OPT, true, "Cluster name");

        metadataOptions.addOptionGroup(group);
        metadataOptions.addOption(url);
        metadataOptions.addOption(type);
        metadataOptions.addOption(cluster);

        return metadataOptions;
    }
}
