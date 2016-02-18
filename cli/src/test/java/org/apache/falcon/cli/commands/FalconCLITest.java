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

import org.apache.falcon.client.FalconCLIException;
import org.springframework.shell.Bootstrap;
import org.springframework.shell.core.CommandResult;
import org.springframework.shell.core.JLineShellComponent;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

/**
 * Base class for falcon cli test cases.
 */
public class FalconCLITest {
    protected static JLineShellComponent shell;

    @BeforeClass
    public static void startUp() throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        shell = bootstrap.getJLineShellComponent();
        for(CommandMarker command: shell.getSimpleParser().getCommandMarkers()) {
            if(command instanceof BaseFalconCommands) {
                command.
            }
        }
    }


    public <T> T execute(String command) throws Throwable {
        CommandResult commandResult = shell.executeCommand(command);
        if (commandResult.isSuccess()) {
            return (T) commandResult.getResult();
        }
        if (commandResult.getException() != null) {
            throw commandResult.getException();
        }
        throw new FalconCLIException("Result is not success and exception is null");
    }

    public <T> void execute(String command, T result) throws Throwable {
        Assert.assertEquals(execute(command), result);
    }

    public <T> void execute(String command, T result, Throwable throwable) throws Throwable {
        CommandResult commandResult = shell.executeCommand(command);
        if (commandResult.isSuccess()) {
            Assert.assertNull(throwable);
            Assert.assertEquals(commandResult.getResult(), result);
        } else {
            Assert.assertNull(result);
            Assert.assertEquals(commandResult.getException().getClass(), throwable.getClass());
            Assert.assertEquals(commandResult.getException().getMessage(), throwable.getMessage());
        }
    }
}
