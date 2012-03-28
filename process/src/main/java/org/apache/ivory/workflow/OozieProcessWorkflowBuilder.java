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
import org.apache.ivory.converter.OozieProcessMapper;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.parser.Frequency;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;

import java.util.*;

public class OozieProcessWorkflowBuilder extends OozieWorkflowBuilder<Process> {

    private static final String[] COORD_TAGS = {"DEFAULT", "LATE1"}; 
    
    @Override
    public Map<String, Object> newWorkflowSchedule(Process process) throws IvoryException {

        String clusterName = process.getCluster().getName();
        Cluster cluster = configStore.get(EntityType.CLUSTER, clusterName);
        Path bundlePath = new Path(ClusterHelper.getLocation(cluster, "staging") +
                process.getStagingPath());

        OozieProcessMapper mapper = new OozieProcessMapper(process);
        mapper.map(cluster, bundlePath);

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
    public List<ExternalId> getExternalIds(Process process, String cluster, Date start, Date end) throws IvoryException {
        TimeZone timezone = EntityUtil.getTimeZone(process.getValidity().getTimezone());

        Calendar procStart = Calendar.getInstance(timezone);
        procStart.setTime(getNextStartTime(process, cluster, start));
        
        Frequency freq = Frequency.valueOf(process.getFrequency());
        List<ExternalId> extIds = new ArrayList<ExternalId>();
        while(procStart.getTime().before(end)) {
            for(String tag:COORD_TAGS)
                extIds.add(new ExternalId(process.getName(), tag, procStart.getTime()));
            procStart.add(freq.getTimeUnit().getCalendarUnit(), process.getPeriodicity());
        }
        return extIds;
    }

    @Override
    public Date getNextStartTime(Process process, String cluster, Date now) throws IvoryException {
        return getNextStartTime(EntityUtil.parseDateUTC(process.getValidity().getStart()), Frequency.valueOf(process.getFrequency()), 
                process.getPeriodicity(), process.getValidity().getTimezone(), now);
    }
    
    @Override
    public int getConcurrency(Process process) {
        return process.getConcurrency();
    }

    @Override
    public String getEndTime(Process process, String cluster) {
        return process.getValidity().getEnd();
    }

    @Override
    public void setStartDate(Process process, String cluster, Date startDate) {
        process.getValidity().setStart(EntityUtil.formatDateUTC(startDate));
    }

    @Override
    public void setConcurrency(Process process, int concurrency) {
        process.setConcurrency(concurrency);
    }

    @Override
    public void setEndTime(Process process, String cluster, Date endDate) {
        process.getValidity().setEnd(EntityUtil.formatDateUTC(endDate));
    }
}