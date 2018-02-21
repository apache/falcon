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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.rerun.event.RerunEvent;
import org.apache.falcon.rerun.policy.AbstractRerunPolicy;
import org.apache.falcon.rerun.policy.ExpBackoffPolicy;
import org.apache.falcon.rerun.queue.DelayedQueue;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for a rerun consumer.
 *
 * @param <T> a rerun event
 * @param <M> a rerun handler
 */
public abstract class AbstractRerunConsumer<T extends RerunEvent, M extends AbstractRerunHandler<T, DelayedQueue<T>>>
        implements Runnable {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractRerunConsumer.class);

    protected M handler;

    public AbstractRerunConsumer(M handler) {
        this.handler = handler;
    }

    @Override
    public void run() {
        int attempt = 1;
        AbstractRerunPolicy policy = new ExpBackoffPolicy();
        Frequency frequency = new Frequency("minutes(1)");
        while (!Thread.currentThread().isInterrupted()) {
            T message = null;
            try {
                try {
                    message = handler.takeFromQueue();
                    attempt = 1;
                } catch (FalconException e) {
                    if (ExceptionUtils.getRootCause(e) instanceof InterruptedException){
                        LOG.info("Rerun handler daemon has been interrupted");
                        return;
                    } else {
                        LOG.error("Error while reading message from the queue", e);
                        GenericAlert.alertRerunConsumerFailed(
                                "Error while reading message from the queue: ", e);
                        Thread.sleep(policy.getDelay(frequency, attempt));
                        handler.reconnect();
                        attempt++;
                        continue;
                    }
                }

                // Login the user to access WfEngine as this user
                CurrentUser.authenticate(message.getWorkflowUser());
                AbstractWorkflowEngine wfEngine = handler.getWfEngine(message.getEntityType(),
                        message.getEntityName(), message.getWorkflowUser());
                String jobStatus = wfEngine.getWorkflowStatus(
                        message.getClusterName(), message.getWfId());
                handleRerun(message.getClusterName(), jobStatus, message,
                        message.getEntityType(), message.getEntityName());

            } catch (Throwable e) {
                if (e instanceof EntityNotRegisteredException) {
                    LOG.warn("Entity {} of type {} doesn't exist in config store. Rerun "
                                    + "cannot be done for workflow ", message.getEntityName(),
                            message.getEntityType(), message.getWfId());
                    continue;
                }
                LOG.error("Error in rerun consumer", e);
            }
        }
    }

    protected abstract void handleRerun(String clusterName, String jobStatus, T message,
                                        String entityType, String entityName);
}
