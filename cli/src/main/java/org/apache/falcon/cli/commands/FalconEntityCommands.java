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

import org.apache.falcon.ResponseHelper;
import org.apache.falcon.cli.FalconCLIRuntimeException;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import java.io.File;

import static org.apache.falcon.cli.FalconCLI.COLO_OPT;
import static org.apache.falcon.cli.FalconCLI.END_OPT;
import static org.apache.falcon.cli.FalconCLI.ENTITY_NAME_OPT;
import static org.apache.falcon.cli.FalconCLI.FILE_PATH_OPT;
import static org.apache.falcon.cli.FalconCLI.START_OPT;
import static org.apache.falcon.cli.FalconCLI.TYPE_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.*;
import static org.apache.falcon.cli.FalconEntityCLI.END_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.ENTITY_NAME_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.FILE_PATH_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.SKIPDRYRUN_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.SKIPDRYRUN_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.START_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.TYPE_OPT_DESCRIPTION;

/**
 * Entity Commands.
 */
public class FalconEntityCommands extends BaseFalconCommands {
    @CliCommand(value = "entity " + SLA_MISS_ALERT_OPT, help = SLA_MISS_ALERT_OPT_DESCRIPTION)
    public String slaAlert(
            @CliOption(key = {"", TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {"", ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {"", START_OPT}, mandatory = true, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {"", END_OPT}, mandatory = true, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {"", COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION) final String colo
    ) {
        SchedulableEntityInstanceResult response = null;
        try {
            response = getFalconClient().getFeedSlaMissPendingAlerts(entityType,
                    entityName, start, end, getColo(colo));
        } catch (FalconCLIException e) {
            throw new FalconCLIRuntimeException(e);
        }
        return ResponseHelper.getString(response);
    }

    @CliCommand(value = "entity " + SUBMIT_OPT, help = SUBMIT_OPT_DESCRIPTION)
    public String submit(
            @CliOption(key = {"", TYPE_OPT}, mandatory = true, help = "<type>") final String entityType,
            @CliOption(key = {"", FILE_PATH_OPT}, mandatory = true, help = "<file-path>") final File filePath
    ) {
        try {
            return getFalconClient().submit(entityType, filePath.getPath(), getDoAs()).getMessage();
        } catch (FalconCLIException e) {
            throw new FalconCLIRuntimeException(e);
        }
    }

    @CliCommand(value = "entity " + LOOKUP_OPT, help = LOOKUP_OPT_DESCRIPTION)
    public String lookup(
            @CliOption(key = {"", TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {"", PATH_OPT}, mandatory = true, help = PATH_OPT_DESCRIPTION) final File feedInstancePath
    ) {
        try {
            FeedLookupResult resp = getFalconClient().reverseLookUp(entityType, feedInstancePath.getPath(), getDoAs());
            return ResponseHelper.getString(resp);
        } catch (FalconCLIException e) {
            throw new FalconCLIRuntimeException(e);
        }
    }

    @CliCommand(value = "entity " + UPDATE_OPT, help = UPDATE_OPT_DESCRIPTION)
    public String lookup(
            @CliOption(key = {"", TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {"", ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {"", FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION)
            final File filePath,
            @CliOption(key = {"", SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun
    ) {
        try {
            return getFalconClient()
                    .update(entityType, entityName, filePath.getPath(), skipDryRun, getDoAs()).getMessage();
        } catch (FalconCLIException e) {
            throw new FalconCLIRuntimeException(e);
        }
    }
}
