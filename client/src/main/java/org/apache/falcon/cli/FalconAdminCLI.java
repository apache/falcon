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
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Admin extension to Falcon Command Line Interface - wraps the RESTful API for admin commands.
 */
public class FalconAdminCLI extends FalconCLI {

    private static final String STACK_OPTION = "stack";

    public FalconAdminCLI() throws Exception {
        super();
    }

    public Options createAdminOptions() {
        Options adminOptions = new Options();
        Option url = new Option(URL_OPTION, true, "Falcon URL");
        adminOptions.addOption(url);

        OptionGroup group = new OptionGroup();
        Option status = new Option(STATUS_OPT, false,
                "show the current system status");
        Option version = new Option(VERSION_OPT, false,
                "show Falcon server build version");
        Option stack = new Option(STACK_OPTION, false,
                "show the thread stack dump");
        Option doAs = new Option(DO_AS_OPT, true,
                "doAs user");
        Option help = new Option("help", false, "show Falcon help");
        Option debug = new Option(DEBUG_OPTION, false, "Use debug mode to see debugging statements on stdout");
        group.addOption(status);
        group.addOption(version);
        group.addOption(stack);
        group.addOption(help);

        adminOptions.addOptionGroup(group);
        adminOptions.addOption(doAs);
        adminOptions.addOption(debug);
        return adminOptions;
    }

    public int adminCommand(CommandLine commandLine, FalconClient client,
                             String falconUrl) throws FalconCLIException, IOException {
        String result;
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String doAsUser = commandLine.getOptionValue(DO_AS_OPT);

        if (optionsList.contains(STACK_OPTION)) {
            result = client.getThreadDump(doAsUser);
            OUT.get().println(result);
        }

        int exitValue = 0;
        if (optionsList.contains(STATUS_OPT)) {
            try {
                int status = client.getStatus(doAsUser);
                if (status != 200) {
                    ERR.get().println("Falcon server is not fully operational (on " + falconUrl + "). "
                            + "Please check log files.");
                    exitValue = status;
                } else {
                    OUT.get().println("Falcon server is running (on " + falconUrl + ")");
                }
            } catch (Exception e) {
                ERR.get().println("Falcon server doesn't seem to be running on " + falconUrl);
                exitValue = -1;
            }
        } else if (optionsList.contains(VERSION_OPT)) {
            result = client.getVersion(doAsUser);
            OUT.get().println("Falcon server build version: " + result);
        } else if (optionsList.contains(HELP_CMD)) {
            OUT.get().println("Falcon Help");
        }
        return exitValue;
    }

}
