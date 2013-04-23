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

import org.apache.falcon.aspect.GenericAlert;
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
    protected void handleRerun(String cluster, String jobStatus,
                               RetryEvent message) {
        try {
            if (!jobStatus.equals("KILLED")) {
                LOG.debug("Re-enqueing message in RetryHandler for workflow with same delay as job status is running:"
                        + message.getWfId());
                message.setMsgInsertTime(System.currentTimeMillis());
                handler.offerToQueue(message);
                return;
            }
            LOG.info("Retrying attempt:"
                    + (message.getRunId() + 1)
                    + " out of configured: "
                    + message.getAttempts()
                    + " attempt for instance::"
                    + message.getEntityName()
                    + ":"
                    + message.getInstance()
                    + " And WorkflowId: "
                    + message.getWfId()
                    + " At time: "
                    + SchemaHelper.formatDateUTC(new Date(System
                    .currentTimeMillis())));
            handler.getWfEngine().reRun(message.getClusterName(),
                    message.getWfId(), null);
        } catch (Exception e) {
            int maxFailRetryCount = Integer.parseInt(StartupProperties.get()
                    .getProperty("max.retry.failure.count", "1"));
            if (message.getFailRetryCount() < maxFailRetryCount) {
                LOG.warn(
                        "Retrying again for process instance "
                                + message.getEntityName() + ":"
                                + message.getInstance() + " after "
                                + message.getDelayInMilliSec()
                                + " seconds as Retry failed with message:", e);
                message.setFailRetryCount(message.getFailRetryCount() + 1);
                try {
                    handler.offerToQueue(message);
                } catch (Exception ex) {
                    LOG.error("Unable to re-offer to queue:", ex);
                    GenericAlert.alertRetryFailed(message.getEntityType(),
                            message.getEntityName(), message.getInstance(),
                            message.getWfId(),
                            Integer.toString(message.getRunId()),
                            ex.getMessage());
                }
            } else {
                LOG.warn(
                        "Failure retry attempts exhausted for instance: "
                                + message.getEntityName() + ":"
                                + message.getInstance(), e);
                GenericAlert.alertRetryFailed(message.getEntityType(),
                        message.getEntityName(), message.getInstance(),
                        message.getWfId(),
                        Integer.toString(message.getRunId()),
                        "Failure retry attempts exhausted");
            }
        }
    }
}
