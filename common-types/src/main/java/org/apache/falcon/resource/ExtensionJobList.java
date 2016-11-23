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
import java.util.ArrayList;
import java.util.List;

/**
 * Extension job list used for marshalling / unmarshalling with REST calls.
 */
//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ExtensionJobList {

    @XmlElement
    public int numJobs;

    @XmlElementWrapper(name = "jobs")
    public List<JobElement> job;

    public ExtensionJobList() {
        numJobs = 0;
        job = null;
    }

    public ExtensionJobList(int numJobs) {
        this.numJobs = numJobs;
        job = new ArrayList<JobElement>();
    }

    public ExtensionJobList(int numJobs, List<JobElement> elements) {
        this.numJobs = numJobs;
        this.job = elements;
    }

    public void addJob(JobElement element) {
        job.add(element);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(numJobs + "\n\n");
        for (JobElement element : job) {
            buffer.append(element.toString());
        }
        return buffer.toString();
    }

    /**
     * Element for a job.
     */
    public static class JobElement {
        @XmlElement
        public String jobName;

        @XmlElement
        public EntityList jobEntities;

        public JobElement() {
            jobName = null;
            jobEntities = null;
        }

        public JobElement(String name, EntityList entities) {
            jobName = name;
            jobEntities = entities;
        }

        @Override
        public String toString() {
            StringBuilder buffer = new StringBuilder();
            buffer.append("Job: " + jobName + ", #. entities: ");
            buffer.append(jobEntities.toString() + "\n");
            return buffer.toString();
        }
    }
}
