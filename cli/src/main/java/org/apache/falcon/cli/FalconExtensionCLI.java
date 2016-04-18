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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Falcon extensions Command Line Interface - wraps the RESTful API for extensions.
 */
public class FalconExtensionCLI {
    public static final AtomicReference<PrintStream> OUT = new AtomicReference<>(System.out);

    // Extension artifact repository Commands
    public static final String ENUMERATE_OPT = "enumerate";
    public static final String DEFINITION_OPT = "definition";
    public static final String DESCRIBE_OPT = "describe";

    public static final String ENTENSION_NAME_OPT = "extensionName";

    public FalconExtensionCLI() {
    }

    public void extensionCommand(CommandLine commandLine, FalconClient client) throws FalconCLIException {
        Set<String> optionsList = new HashSet<>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String extensionName = commandLine.getOptionValue(ENTENSION_NAME_OPT);

        if (optionsList.contains(ENUMERATE_OPT)) {
            result = client.enumerateExtensions();
            prettyPrintJson(result);
        } else if (optionsList.contains(DEFINITION_OPT)) {
            validateExtensionName(extensionName);
            result = client.getExtensionDefinition(extensionName);
            prettyPrintJson(result);
        } else if (optionsList.contains(DESCRIBE_OPT)) {
            validateExtensionName(extensionName);
            result = client.getExtensionDescription(extensionName);
            OUT.get().println(result);
        } else {
            throw new FalconCLIException("Invalid extension command");
        }
    }

    public Options createExtensionOptions() {
        Options extensionOptions = new Options();

        OptionGroup group = new OptionGroup();
        Option enumerate = new Option(ENUMERATE_OPT, false, "Enumerate all extensions");
        Option definition = new Option(DEFINITION_OPT, false, "Get extension definition");
        Option describe = new Option(DESCRIBE_OPT, false, "Get extension description");
        group.addOption(enumerate);
        group.addOption(definition);
        group.addOption(describe);

        extensionOptions.addOptionGroup(group);

        Option name = new Option(ENTENSION_NAME_OPT, true, "Extension name");
        extensionOptions.addOption(name);

        return extensionOptions;
    }

    private void validateExtensionName(final String extensionName) throws FalconCLIException {
        if (StringUtils.isBlank(extensionName)) {
            throw new FalconCLIException("Extension name cannot be null or empty");
        }
    }

    private static void prettyPrintJson(final String jsonString) {
        if (StringUtils.isBlank(jsonString)) {
            OUT.get().println("No result returned");
            return;
        }
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(jsonString.trim());
        OUT.get().println(gson.toJson(je));
    }
}
