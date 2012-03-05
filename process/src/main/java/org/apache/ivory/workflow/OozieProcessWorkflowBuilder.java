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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.converter.OozieProcessMapper;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.parser.ProcessEntityParser.Frequency;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;

public class OozieProcessWorkflowBuilder extends OozieWorkflowBuilder<Process> {

    @Override
    public Map<String, Object> newWorkflowSchedule(Process process) throws IvoryException {

        String clusterName = process.getCluster().getName();
        Cluster cluster = configStore.get(EntityType.CLUSTER, clusterName);
        Path workflowPath = new Path(ClusterHelper.getLocation(cluster, "staging") +
                process.getStagingPath());

        OozieProcessMapper converter = new OozieProcessMapper(process);
        Path bundlePath = converter.convert(cluster, workflowPath);

        List<Cluster> clusters = new ArrayList<Cluster>();
        List<Path> paths = new ArrayList<Path>();
        clusters.add(cluster);
        paths.add(bundlePath);
        return createAppProperties(clusters, paths);
    }

    @Override
    public Cluster[] getScheduledClustersFor(Process process) throws IvoryException {

        // TODO asserts
        String clusterName = process.getCluster().getName();
        Cluster cluster = configStore.get(EntityType.CLUSTER, clusterName);
        return new Cluster[] { cluster };
    }
    
    @Override
    public List<ExternalId> getExternalIds(Entity entity, Date start, Date end) throws IvoryException {
        Process process = (Process) entity;
        
        TimeZone timezone = EntityUtil.getTimeZone(process.getValidity().getTimezone());
        Calendar procStart = Calendar.getInstance(timezone);
        procStart.setTime(EntityUtil.parseDateUTC(process.getValidity().getStart()));
        Calendar startCal = Calendar.getInstance(timezone);
        startCal.setTime(start);
        Calendar endCal = Calendar.getInstance(timezone);
        endCal.setTime(end);
        
        Frequency freq = Frequency.valueOf(process.getFrequency());
        List<ExternalId> extIds = new ArrayList<ExternalId>();
        while(procStart.before(startCal)) {
            procStart.add(freq.getTimeUnit().getCalendarUnit(), Integer.valueOf(process.getPeriodicity()));
        }
        
        while(procStart.before(endCal)) {
            extIds.add(new ExternalId(process.getName(), procStart.getTime()));
            procStart.add(freq.getTimeUnit().getCalendarUnit(), Integer.valueOf(process.getPeriodicity()));
        }
        return extIds;
    }
}