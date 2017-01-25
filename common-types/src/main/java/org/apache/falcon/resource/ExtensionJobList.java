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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;

/**
 * Extension job list used for marshalling / unmarshalling with REST calls.
 */
//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ExtensionJobList {

    @XmlElement
    int numJobs;

    @XmlElementWrapper(name = "jobs")
    public Map<String, String> job;

    public ExtensionJobList() {
        numJobs = 0;
        job = null;
    }

    public int getNumJobs() {
        return numJobs;
    }

    public ExtensionJobList(int numJobs) {
        this.numJobs = numJobs;
        job = new HashMap<>();
    }

    public ExtensionJobList(int numJobs, Map<String, String> extensionJobNames) {
        this.numJobs = numJobs;
        this.job = extensionJobNames;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(numJobs).append("\n");
        for (Map.Entry<String, String> extensionJobs : job.entrySet()) {
            builder.append(extensionJobs.getKey() + "\t" + extensionJobs.getValue() + "\n");
        }
        return builder.toString();
    }
}
