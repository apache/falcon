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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.parser.Frequency;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.security.CurrentUser;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public abstract class OozieWorkflowBuilder<T extends Entity> extends WorkflowBuilder<T> {

    private static Logger LOG = Logger.getLogger(OozieWorkflowBuilder.class);

    protected static final ConfigurationStore configStore = ConfigurationStore.get();

    protected Map<String, Object> createAppProperties(List<Cluster> clusters,
                                                      List<Path> paths) throws IvoryException {

        Map<String, Object> map = new HashMap<String, Object>();
        List<Properties> propList = new ArrayList<Properties>();

        assert clusters.size() == paths.size() : "Cluster list and path list must be equal in size";
        for (int index = 0; index < clusters.size(); index++) {
            Path path = paths.get(index);
            Properties properties = new Properties();
            properties.setProperty(OozieWorkflowEngine.NAME_NODE,
                    ClusterHelper.getHdfsUrl(clusters.get(index)));
            properties.setProperty(OozieWorkflowEngine.JOB_TRACKER,
                    ClusterHelper.getMREndPoint(clusters.get(index)));
            properties.setProperty(OozieClient.BUNDLE_APP_PATH,
                    "${" + OozieWorkflowEngine.NAME_NODE + "}" + path.toString());

            properties.setProperty(OozieClient.USER_NAME, CurrentUser.getUser());
            
            //Ivory common jars to be used.
            properties.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");

            propList.add(properties);
            LOG.info("Cluster: " + clusters.get(index).getName() + ", PROPS: " + properties);
        }
        map.put(PROPS, propList);
        map.put(CLUSTERS, clusters);
        return map;
    }
    
    public abstract int getConcurrency(T entity);

    public abstract void setConcurrency(T entity, int concurrency);

    public abstract String getEndTime(T entity, String cluster);

    public abstract void setEndTime(T entity, String cluster, Date endDate);

    public abstract void setStartDate(T entity, String cluster, Date startDate);
    
    public abstract Date getNextStartTime(T entity, String cluster, Date now) throws IvoryException;
    
    protected Date getNextStartTime(Date startTime, Frequency frequency, int periodicity, String timzone, Date now) {
        Calendar startCal = Calendar.getInstance(EntityUtil.getTimeZone(timzone));
        startCal.setTime(startTime);
        
        while(startCal.getTime().compareTo(now) <= 0) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(), periodicity);
        }
        return startCal.getTime();
        
    }
}
