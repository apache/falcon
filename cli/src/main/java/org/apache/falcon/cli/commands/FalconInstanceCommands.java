/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.cli.commands;

import static org.apache.falcon.cli.FalconCLI.*;
import static org.apache.falcon.cli.FalconInstanceCLI.*;

import org.apache.falcon.ResponseHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

/**
 * Instance commands.
 */
@Component
public class FalconInstanceCommands extends BaseFalconCommands {
    public static final String INSTANCE_PREFIX = "entity";
    public static final String INSTANCE_COMMAND_PREFIX = INSTANCE_PREFIX + " ";

    @CliCommand(value = INSTANCE_COMMAND_PREFIX + TRIAGE_OPT, help = TRIAGE_OPT_DESCRIPTION)
    public String triage(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start
    ) {
        return getFalconClient().triage(entityType.name(), entityName, start, getColo(colo)).toString();
    }

    @CliCommand(value = INSTANCE_COMMAND_PREFIX + DEPENDENCY_OPT, help = DEPENDENCY_OPT_DESCRIPTION)
    public String dependency(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start
    ) {
        return getFalconClient().getInstanceDependencies(entityType.name(), entityName, start, getColo(colo))
                .toString();
    }

    @CliCommand(value = INSTANCE_COMMAND_PREFIX + RUNNING_OPT, help = RUNNING_OPT_DESCRIPTION)
    public String running(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start
    ) {
        return ResponseHelper.getString(getFalconClient().getRunningInstances(type,
                entity, colo, lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, doAsUser));
    }
}
