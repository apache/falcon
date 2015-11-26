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
package org.apache.falcon.tools;


import org.apache.falcon.state.AbstractSchedulerTestBase;
import org.apache.falcon.util.BuildProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Tests for DB operations tool.
 */
public class TestFalconStateStoreDBCLI extends AbstractSchedulerTestBase {

    @BeforeClass
    public void setup() throws Exception {
        super.setup();
    }

    @AfterClass
    public void cleanup() throws IOException {
        super.cleanup();
    }


    @Test
    public void testFalconDBCLI() throws Exception {
        File sqlFile = new File(DB_SQL_FILE);
        String[] argsCreate = { "create", "-sqlfile", sqlFile.getAbsolutePath(), "-run" };
        int result = execDBCLICommands(argsCreate);
        Assert.assertEquals(0, result);
        Assert.assertTrue(sqlFile.exists());

        ByteArrayOutputStream data = new ByteArrayOutputStream();
        PrintStream oldOut = System.out;
        try {
            // show versions
            System.setOut(new PrintStream(data));
            String[] argsVersion = { "version" };
            Assert.assertEquals(0, execDBCLICommands(argsVersion));
            Assert.assertTrue(data.toString().contains("db.version: "
                    + BuildProperties.get().getProperty("project.version")));
            // show help information
            data.reset();
            String[] argsHelp = { "help" };
            Assert.assertEquals(0, execDBCLICommands(argsHelp));
            Assert.assertTrue(data.toString().contains("falcondb create <OPTIONS> : Create Falcon DB schema"));
            Assert.assertTrue(data.toString().contains("falcondb upgrade <OPTIONS> : Upgrade Falcon DB schema"));
            // try run invalid command
            data.reset();
            String[] argsInvalidCommand = { "invalidCommand" };
            Assert.assertEquals(1, execDBCLICommands(argsInvalidCommand));
        } finally {
            System.setOut(oldOut);
        }
        // generate an upgrade script
        File update = new File(DB_UPDATE_SQL_FILE);

        String[] argsUpgrade = { "upgrade", "-sqlfile", update.getAbsolutePath(), "-run" };
        BuildProperties.get().setProperty("project.version", "99999-SNAPSHOT");
        Assert.assertEquals(0, execDBCLICommands(argsUpgrade));

        Assert.assertTrue(update.exists());
    }

}
