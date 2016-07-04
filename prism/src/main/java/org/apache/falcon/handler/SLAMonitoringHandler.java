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
package org.apache.falcon.handler;


import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.service.EntitySLAMonitoringService;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Class for handling workflow notifications to monitor SLA.
 */
public class SLAMonitoringHandler implements WorkflowExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(SLAMonitoringHandler.class);

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {
        if (context.hasWorkflowSucceeded()) {
            if (context.getEntityType().toString().equals(EntityType.FEED.name())){
                updateSLAMonitoring(context.getClusterName(), context.getOutputFeedNamesList(),
                    context.getOutputFeedInstancePathsList());
            }
            if (context.getEntityType().toString().equals(EntityType.PROCESS.name())){
                EntitySLAMonitoringService.get().makeProcessInstanceAvailable(context.getClusterName(),
                        context.getEntityName(), context.getNominalTimeAsISO8601(), context.getEntityType());
            }
        }
    }

    private void updateSLAMonitoring(String clusterName, String[] outputFeedNamesList,
                                     String[] outputFeedInstancePathsList) throws FalconException {
        Storage storage;
        for (int index=0; index<outputFeedNamesList.length; index++) {
            if (!StringUtils.equals(outputFeedNamesList[index], "NONE")) {
                Feed feed = EntityUtil.getEntity(EntityType.FEED, outputFeedNamesList[index]);
                storage = FeedHelper.createStorage(clusterName, feed);
                String templatePath = new Path(storage.getUriTemplate(LocationType.DATA)).toUri().getPath();
                Date date = FeedHelper.getDate(templatePath, new Path(outputFeedInstancePathsList[index]),
                    EntityUtil.getTimeZone(feed));
                EntitySLAMonitoringService.get().makeFeedInstanceAvailable(outputFeedNamesList[index],
                        clusterName, date);
            }
        }
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        // do nothing since nothing to update in SLA Monitoring
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException {
        // do nothing since nothing to update in SLA Monitoring
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException {
        // do nothing since nothing to update in SLA Monitoring
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException {
        // do nothing since nothing to update in SLA Monitoring
    }
}
