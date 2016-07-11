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
package org.apache.falcon.state;

import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.service.FalconJPAService;
import org.apache.falcon.tools.FalconStateStoreDBCLI;
import org.apache.falcon.util.StateStoreProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;

/**
 * TestBase for tests using Falcon Native Scheduler.
 */
public class AbstractSchedulerTestBase extends AbstractTestBase {
    private static final String DB_BASE_DIR = "target/test-data/falcondb";
    protected static String dbLocation = DB_BASE_DIR + File.separator + "data.db";
    protected static String url = "jdbc:derby:"+ dbLocation +";create=true";
    protected static final String DB_SQL_FILE = DB_BASE_DIR + File.separator + "out.sql";
    protected static final String DB_UPDATE_SQL_FILE = DB_BASE_DIR + File.separator + "update.sql";
    protected LocalFileSystem fs = new LocalFileSystem();

    public void setup() throws Exception {
        StateStoreProperties.get().setProperty(FalconJPAService.URL, url);
        Configuration localConf = new Configuration();
        fs.initialize(LocalFileSystem.getDefaultUri(localConf), localConf);
        fs.mkdirs(new Path(DB_BASE_DIR));
    }

    public void cleanup() throws IOException {
        cleanupDB();
    }

    private void cleanupDB() throws IOException {
        fs.delete(new Path(DB_BASE_DIR), true);
    }

    public void createDB(String file) {
        File sqlFile = new File(file);
        String[] argsCreate = { "create", "-sqlfile", sqlFile.getAbsolutePath(), "-run" };
        int result = execDBCLICommands(argsCreate);
        Assert.assertEquals(0, result);
        Assert.assertTrue(sqlFile.exists());

    }

    protected int execDBCLICommands(String[] args) {
        return new FalconStateStoreDBCLI().run(args);
    }
}
