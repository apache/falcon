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

package org.apache.falcon.adfservice;

import org.apache.falcon.FalconException;

/**
 * Azure ADF Pig Job.
 */
public class ADFPigJob extends ADFJob {
    private static final String PIG_SCRIPT_EXTENSION = ".pig";
    private static final String ENGINE_TYPE = "pig";
    private static final String INPUT_FEED_SUFFIX = "-pig-input-feed";
    private static final String OUTPUT_FEED_SUFFIX = "-pig-output-feed";

    private String pigScriptPath;
    private DataFeed inputDataFeed;
    private DataFeed outputDataFeed;

    public ADFPigJob(String message, String id) throws FalconException {
        super(message, id);
        type = JobType.PIG;
        inputDataFeed = getInputFeed();
        outputDataFeed = getOutputFeed();
        pigScriptPath = activityHasScriptPath() ? getScriptPath() : createScriptFile(PIG_SCRIPT_EXTENSION);
    }

    @Override
    public void startJob() throws FalconException {
        startProcess(inputDataFeed, outputDataFeed, ENGINE_TYPE, pigScriptPath);
    }

    @Override
    public void cleanup() throws FalconException {
        cleanupProcess(inputDataFeed, outputDataFeed);
    }

    private DataFeed getInputFeed() throws FalconException {
        return getFeed(jobEntityName() + INPUT_FEED_SUFFIX, getInputTables().get(0),
                getTableCluster(getInputTables().get(0)));
    }

    private DataFeed getOutputFeed() throws FalconException {
        return getFeed(jobEntityName() + OUTPUT_FEED_SUFFIX, getOutputTables().get(0),
                getTableCluster(getOutputTables().get(0)));
    }

    private DataFeed getFeed(final String feedName, final String tableName,
                             final String clusterName) throws FalconException {
        return new DataFeed.Builder().withFeedName(feedName).withFrequency(frequency)
                .withClusterName(clusterName).withStartTime(startTime).withEndTime(endTime)
                .withAclOwner(proxyUser).withLocationPath(getADFTablePath(tableName)).build();
    }
}
