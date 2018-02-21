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
package org.apache.falcon.rerun.handler;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.rerun.event.RetryEvent;
import org.apache.falcon.rerun.queue.DelayedQueue;
import org.apache.falcon.util.StartupProperties;

import java.util.Date;

/**
 * A consumer of retry events which reruns the workflow in the workflow engine.
 *
 * @param <T>
 */
public class RetryConsumer<T extends RetryHandler<DelayedQueue<RetryEvent>>>
        extends AbstractRerunConsumer<RetryEvent, T> {

    public RetryConsumer(T handler) {
        super(handler);
    }

    @Override
    protected void handleRerun(String clusterName, String jobStatus,
                               RetryEvent message, String entityType, String entityName) {
        try {
            // Can happen when user does a manual re-run in-between retries.
            if (jobStatus.equals("RUNNING") || jobStatus.equals("PREP")) {
                LOG.debug("Re-enqueing message in RetryHandler for workflow with same delay as "
                        + "job status is {} for : {}", jobStatus, message.getWfId());
                message.setMsgInsertTime(System.currentTimeMillis());
                handler.offerToQueue(message);
                return;
            } else if (jobStatus.equals("SUSPENDED") || jobStatus.equals("SUCCEEDED")) {
                LOG.debug("Not retrying workflow {} anymore as it is in {} state. ", message.getWfId(), jobStatus);
                return;
            }
            LOG.info("Retrying attempt: {} out of configured: {} attempts for instance: {}:{} And WorkflowId: {}"
                            + " At time: {}",
                    (message.getRunId() + 1), message.getAttempts(), message.getEntityName(), message.getInstance(),
                    message.getWfId(), SchemaHelper.formatDateUTC(new Date(System.currentTimeMillis())));
            // Use coord action id for rerun if available
            String id = message.getWfId();
            if (!id.contains("-C@") && StringUtils.isNotBlank(message.getParentId())) {
                id = message.getParentId();
            }

            handler.getWfEngine(entityType, entityName, message.getWorkflowUser())
                    .reRun(message.getClusterName(), id, null, false);
        } catch (Exception e) {
            if (e instanceof EntityNotRegisteredException) {
                LOG.warn("Entity {} of type {} doesn't exist in config store. So retry "
                        + "cannot be done for workflow ", entityName, entityType, message.getWfId());

                return;
            }
            int maxFailRetryCount = Integer.parseInt(StartupProperties.get()
                    .getProperty("max.retry.failure.count", "1"));
            if (message.getFailRetryCount() < maxFailRetryCount) {
                LOG.warn("Retrying again for process instance {}:{} after {} seconds as Retry failed",
                        message.getEntityName(), message.getInstance(), message.getDelayInMilliSec(), e);
                message.setFailRetryCount(message.getFailRetryCount() + 1);
                try {
                    handler.offerToQueue(message);
                } catch (Exception ex) {
                    LOG.error("Unable to re-offer to queue", ex);
                    GenericAlert.alertRetryFailed(message.getEntityType(),
                            message.getEntityName(), message.getInstance(),
                            message.getWfId(), message.getWorkflowUser(),
                            Integer.toString(message.getRunId()),
                            ex.getMessage());
                }
            } else {
                LOG.warn("Failure retry attempts exhausted for instance: {}:{}",
                        message.getEntityName(), message.getInstance(), e);
                GenericAlert.alertRetryFailed(message.getEntityType(),
                        message.getEntityName(), message.getInstance(),
                        message.getWfId(), message.getWorkflowUser(),
                        Integer.toString(message.getRunId()),
                        "Failure retry attempts exhausted");
            }
        }
    }
}
