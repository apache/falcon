/*
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

package org.apache.ivory.workflow;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.converter.OozieProcessMapper;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.parser.Frequency;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.util.OozieUtils;

import java.util.*;

public class OozieProcessWorkflowBuilder extends OozieWorkflowBuilder<Process> {

    @Override
    public Map<String, Object> newWorkflowSchedule(Process process) throws IvoryException {
        if (!EntityUtil.parseDateUTC(process.getValidity().getStart())
                .before(EntityUtil.parseDateUTC(process.getValidity().getEnd())))
            // start time >= end time
            return null;

        String clusterName = process.getCluster().getName();
        Cluster cluster = configStore.get(EntityType.CLUSTER, clusterName);
        Path bundlePath = new Path(ClusterHelper.getLocation(cluster, "staging"), process.getStagingPath());

        OozieProcessMapper mapper = new OozieProcessMapper(process);
        mapper.map(cluster, bundlePath);

        List<Cluster> clusters = new ArrayList<Cluster>();
        List<Path> paths = new ArrayList<Path>();
        clusters.add(cluster);
        paths.add(bundlePath);
        return createAppProperties(clusters, paths);
    }

    @Override
    public Date getNextStartTime(Process process, String cluster, Date now) throws IvoryException {
        return EntityUtil.getNextStartTime(EntityUtil.parseDateUTC(process.getValidity().getStart()),
                Frequency.valueOf(process.getFrequency()), process.getPeriodicity(), process.getValidity().getTimezone(), now);
    }
}
