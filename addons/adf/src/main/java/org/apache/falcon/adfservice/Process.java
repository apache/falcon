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

import org.apache.falcon.adfservice.util.FSUtils;
import org.apache.falcon.FalconException;

import java.net.URISyntaxException;

/**
 * Class for process.
 */
public class Process {
    private static final String PROCESS_TEMPLATE_FILE = "process.xml";

    private String entityName;
    private String frequency;
    private String startTime;
    private String endTime;
    private String clusterName;
    private String inputFeedName;
    private String outputFeedName;
    private String engineType;
    private String wfPath;
    private String aclOwner;

    public Process(final Builder builder) {
        this.entityName = builder.name;
        this.clusterName = builder.processClusterName;
        this.frequency = builder.processFrequency;
        this.startTime = builder.processStartTime;
        this.endTime = builder.processEndTime;
        this.inputFeedName = builder.processInputFeedName;
        this.outputFeedName = builder.processOutputFeedName;
        this.engineType = builder.processEngineType;
        this.wfPath = builder.processWfPath;
        this.aclOwner = builder.processAclOwner;
    }

    public String getName() {
        return entityName;
    }

    public String getEntityxml() throws FalconException {
        try {
            String template = FSUtils.readHDFSFile(ADFJob.TEMPLATE_PATH_PREFIX, PROCESS_TEMPLATE_FILE);
            return template.replace("$processName$", entityName)
                    .replace("$frequency$", frequency)
                    .replace("$startTime$", startTime)
                    .replace("$endTime$", endTime)
                    .replace("$clusterName$", clusterName)
                    .replace("$inputFeedName$", inputFeedName)
                    .replace("$outputFeedName$", outputFeedName)
                    .replace("$engine$", engineType)
                    .replace("$scriptPath$", wfPath)
                    .replace("$aclowner$", aclOwner);
        } catch (URISyntaxException e) {
            throw new FalconException("Error when generating process xml", e);
        }
    }

    /**
     * Builder for process.
     */
    public static class Builder {
        private String name;
        private String processClusterName;
        private String processFrequency;
        private String processStartTime;
        private String processEndTime;
        private String processInputFeedName;
        private String processOutputFeedName;
        private String processEngineType;
        private String processWfPath;
        private String processAclOwner;

        public Process build() {
            return new Process(this);
        }

        public Builder withProcessName(final String processName) {
            this.name = processName;
            return this;
        }

        public Builder withClusterName(final String clusterName) {
            this.processClusterName = clusterName;
            return this;
        }

        public Builder withFrequency(final String frequency) {
            this.processFrequency = frequency;
            return this;
        }

        public Builder withStartTime(final String startTime) {
            this.processStartTime = startTime;
            return this;
        }

        public Builder withEndTime(final String endTime) {
            this.processEndTime = endTime;
            return this;
        }

        public Builder withInputFeedName(final String inputFeedName) {
            this.processInputFeedName = inputFeedName;
            return this;
        }

        public Builder withOutputFeedName(final String outputFeedName) {
            this.processOutputFeedName = outputFeedName;
            return this;
        }

        public Builder withAclOwner(final String aclOwner) {
            this.processAclOwner = aclOwner;
            return this;
        }

        public Builder withEngineType(final String engineType) {
            this.processEngineType = engineType;
            return this;
        }

        public Builder withWFPath(final String wfPath) {
            this.processWfPath = wfPath;
            return this;
        }
    }

}
