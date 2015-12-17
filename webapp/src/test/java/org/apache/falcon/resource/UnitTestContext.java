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

package org.apache.falcon.resource;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.unit.FalconUnit;
import org.apache.falcon.unit.FalconUnitClient;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Context used to run unit tests using Falcon Unit.
 */
public class UnitTestContext extends AbstractTestContext {

    public static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

    protected String colo;
    protected String clusterName;
    protected String processName;
    protected String processEndTime;
    protected String inputFeedName;
    protected String outputFeedName;

    private static FalconUnitClient client;
    private static FileSystem fs;
    protected static ConfigurationStore configStore;
    protected Map<String, String> overlay;

    public UnitTestContext() throws FalconException, IOException {
        client = FalconUnit.getClient();
        fs = FalconUnit.getFileSystem();
        configStore = client.getConfigStore();
        overlay = getUniqueOverlay();
    }

    public String getProcessName() {
        return processName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getInputFeedName() {
        return inputFeedName;
    }

    public static FalconUnitClient getClient() {
        return client;
    }

    protected void prepare(String workflow) throws Exception {
        mkdir(fs, new Path("/falcon"), new FsPermission((short) 511));

        Path wfParent = new Path("/falcon/test");
        fs.delete(wfParent, true);
        Path wfPath = new Path(wfParent, "workflow");
        mkdir(fs, wfPath);
        mkdir(fs, new Path("/falcon/test/workflow/lib"));
        fs.copyFromLocalFile(false, true,
                new Path(TestContext.class.getResource("/" + workflow).getPath()),
                new Path(wfPath, "workflow.xml"));
        mkdir(fs, new Path(wfParent, "input/2012/04/20/00"));
        mkdir(fs, new Path(wfParent, "input/2012/04/21/00"));
        Path outPath = new Path(wfParent, "output");
        mkdir(fs, outPath, new FsPermission((short) 511));
    }

    protected void prepare() throws Exception {
        prepare("sleepWorkflow.xml");
    }

    public Map<String, String> getUniqueOverlay() throws FalconException {
        Map<String, String> uniqueOverlay = new HashMap<String, String>();
        long time = System.currentTimeMillis();
        clusterName = "cluster" + time;
        uniqueOverlay.put("src.cluster.name", clusterName);
        uniqueOverlay.put("cluster", clusterName);
        uniqueOverlay.put("colo", DeploymentUtil.getCurrentColo());
        colo = DeploymentUtil.getCurrentColo();
        inputFeedName = "in" + time;
        uniqueOverlay.put("inputFeedName", inputFeedName);
        //only feeds with future dates can be scheduled
        Date endDate = new Date(System.currentTimeMillis() + 15 * 60 * 1000);
        uniqueOverlay.put("feedEndDate", SchemaHelper.formatDateUTC(endDate));
        processEndTime = SchemaHelper.formatDateUTC(endDate);
        uniqueOverlay.put("processEndDate", processEndTime);
        outputFeedName = "out" + time;
        uniqueOverlay.put("outputFeedName", outputFeedName);
        processName = "p" + time;
        uniqueOverlay.put("processName", processName);
        uniqueOverlay.put("user", System.getProperty("user.name"));
        uniqueOverlay.put("workflow.path", "/falcon/test/workflow");
        uniqueOverlay.put("workflow.lib.path", "/falcon/test/workflow/lib");
        overlay = uniqueOverlay;
        return uniqueOverlay;
    }
}
