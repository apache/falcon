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

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.converter.OozieProcessMapper;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;

public class OozieProcessWorkflowBuilder extends OozieWorkflowBuilder<Process> {

    @Override
    public Map<String, Properties> newWorkflowSchedule(Process process, List<String> clusters) throws IvoryException {
        Map<String, Path> pathMap = new HashMap<String, Path>();
        
        if (!EntityUtil.parseDateUTC(process.getValidity().getStart())
                .before(EntityUtil.parseDateUTC(process.getValidity().getEnd())))
            // start time >= end time
            return new HashMap<String, Properties>();

        for(String clusterName:clusters) {
            Cluster cluster = configStore.get(EntityType.CLUSTER, clusterName);
            Path bundlePath = new Path(ClusterHelper.getLocation(cluster, "staging"), EntityUtil.getStagingPath(process));
            OozieProcessMapper mapper = new OozieProcessMapper(process);
            if(mapper.map(cluster, bundlePath)==false){
            	continue;
            }           
            pathMap.put(clusterName, bundlePath);
        }
        return createAppProperties(pathMap);
    }

    @Override
    public Date getNextStartTime(Process process, String cluster, Date now) throws IvoryException {
        return EntityUtil.getNextStartTime(EntityUtil.parseDateUTC(process.getValidity().getStart()),
                process.getFrequency(), process.getValidity().getTimezone(), now);
    }

    @Override
    public String[] getWorkflowNames(Process process) {
        return new String[] { EntityUtil.getWorkflowName(Tag.DEFAULT,process).toString() };
    }
}