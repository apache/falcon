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

package org.apache.falcon.regression.core.helpers;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.Builder;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.OSUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * FalconClientBuilder is to be used for launching falcon client command.
 */
public final class FalconClientBuilder implements Builder<CommandLine> {
    private final String user;
    private final CommandLine commandLine;
    private final List<String> args;
    private final SuType suType;

    private enum SuType {
        /**
         * Takes care of switching user on linux. Current implemented through sudo.
         */
        LIN_SUDO {
            @Override
            public CommandLine getCommandLine(String forUser) {
                return CommandLine.parse("sudo").addArgument("-u")
                    .addArgument(forUser).addArgument(FALCON_CLIENT_BINARY);
            }
            @Override
            public void addArgsToCommandLine(CommandLine cmdLine, List<String> arguments) {
                for (String arg : arguments) {
                    cmdLine.addArgument(arg);
                }
            }
        },
        /**
         * Takes care of switching user on windows. Needs to be implemented.
         */
        WIN_SU {
            @Override
            public CommandLine getCommandLine(String forUser) {
                return CommandLine.parse(OSUtil.WIN_SU_BINARY)
                    .addArgument("-u").addArgument(forUser)
                    .addArgument("-p").addArgument(MerlinConstants.getPasswordForUser(forUser))
                    .addArgument(FALCON_CLIENT_BINARY);
            }
            @Override
            public void addArgsToCommandLine(CommandLine cmdLine, List<String> arguments) {
                String lastArg = StringUtils.join(arguments, " ");
                cmdLine.addArgument(lastArg, true);
            }
        },
        /**
         * Takes care of the case where no user switch is required.
         */
        NONE {
            @Override
            public CommandLine getCommandLine(String forUser) {
                return CommandLine.parse(FALCON_CLIENT_BINARY);
            }
            @Override
            public void addArgsToCommandLine(CommandLine cmdLine, List<String> arguments) {
                for (String arg : arguments) {
                    cmdLine.addArgument(arg);
                }
            }
        };

        private static final String FALCON_CLIENT_BINARY =
                Config.getProperty("falcon.client.binary", "falcon");
        public abstract void addArgsToCommandLine(CommandLine cmdLine, List<String> arguments);
        public abstract CommandLine getCommandLine(String forUser);
    }

    private FalconClientBuilder(String user) {
        this.user = user;
        args = new ArrayList<>();
        if (user == null) {
            suType = SuType.NONE;
            commandLine = suType.getCommandLine(null);
        } else {
            if (OSUtil.IS_WINDOWS) {
                suType = SuType.WIN_SU;
                commandLine = suType.getCommandLine(user);
            } else {
                suType = SuType.LIN_SUDO;
                //attempting sudo su - root -c "falcon admin -version"
                commandLine = suType.getCommandLine(user);
            }
        }
    }

    /**
     * Get an instance of FalconClientBuilder.
     * @return instance of FalconClientBuilder
     */
    public static FalconClientBuilder getBuilder() {
        return new FalconClientBuilder(null);
    }

    /**
     * Get an instance of FalconClientBuilder for the given user. It would do commandline
     * construction in a way that the final command is run as given user.
     * @return instance of FalconClientBuilder
     */
    public static FalconClientBuilder getBuilder(String user) {
        return new FalconClientBuilder(user);
    }

    /**
     * Add the given argument.
     * @param arg argument to be added to builder
     * @return this
     */
    private FalconClientBuilder addArg(String arg) {
        args.add(arg);
        return this;
    }

    /**
     * Create submit command.
     * @param entityType type of the entity
     * @param fileName file containing the entity to be submitted
     * @return this
     */
    public FalconClientBuilder getSubmitCommand(String entityType, String fileName) {
        addArg("entity").addArg("-submit");
        addArg("-type").addArg(entityType);
        addArg("-file").addArg(fileName);
        return this;
    }

    /**
     * Create delete command.
     * @param entityType type of the entity
     * @param entityName name of the entity to be deleted
     * @return this
     */
    public FalconClientBuilder getDeleteCommand(String entityType, String entityName) {
        addArg("entity").addArg("-delete");
        addArg("-type").addArg(entityType);
        addArg("-name").addArg(entityName);
        return this;
    }


    /**
     * Build the CommandLine object for this FalconClientBuilder.
     * @return instance of CommandLine object
     */
    @Override
    public CommandLine build() {
        suType.addArgsToCommandLine(commandLine, args);
        return new CommandLine(commandLine);
    }
}
