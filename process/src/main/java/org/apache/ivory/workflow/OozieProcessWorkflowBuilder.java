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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;

import org.apache.ivory.converter.OozieProcessMapper;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ProcessHelper;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.oozie.client.OozieClient;

public class OozieProcessWorkflowBuilder extends OozieWorkflowBuilder<Process> {

    @Override
    public Map<String, Properties> newWorkflowSchedule(Process process, List<String> clusters) throws IvoryException {
        Map<String, Properties> propertiesMap = new HashMap<String, Properties>();

        for (String clusterName: clusters) {
            org.apache.ivory.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, clusterName);
            Properties properties = newWorkflowSchedule(process, processCluster.getValidity().getStart(), clusterName);
            String libPath = process.getWorkflow().getLib();
			if (!StringUtils.isEmpty(libPath)) {
				String path = libPath.replace("${nameNode}", "");
				properties.put(OozieClient.LIBPATH, "${nameNode}" + path);
			}
            if (properties == null) continue;
            propertiesMap.put(clusterName, properties);
        }
        return propertiesMap;
    }

    @Override
    public Properties newWorkflowSchedule(Process process, Date startDate, String clusterName) throws IvoryException {
        org.apache.ivory.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, clusterName);
        if (!startDate.before(processCluster.getValidity().getEnd()))
            // start time >= end time
            return null;

        Cluster cluster = configStore.get(EntityType.CLUSTER, processCluster.getName());
        Path bundlePath = new Path(ClusterHelper.getLocation(cluster, "staging"), EntityUtil.getStagingPath(process));
        Process processClone = (Process) process.clone();
        EntityUtil.setStartDate(processClone, clusterName, startDate);

        OozieProcessMapper mapper = new OozieProcessMapper(processClone);
        if(!mapper.map(cluster, bundlePath)){
            return null;
        }
        return createAppProperties(clusterName, bundlePath);
    }

    @Override
    public Date getNextStartTime(Process process, String cluster, Date now) throws IvoryException {
        org.apache.ivory.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
        return EntityUtil.getNextStartTime(processCluster.getValidity().getStart(),
                process.getFrequency(), process.getTimezone(), now);
    }

    @Override
    public String[] getWorkflowNames(Process process) {
        return new String[] { EntityUtil.getWorkflowName(Tag.DEFAULT,process).toString() };
    }
}