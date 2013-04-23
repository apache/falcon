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

import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.rerun.event.RerunEvent;
import org.apache.falcon.rerun.policy.AbstractRerunPolicy;
import org.apache.falcon.rerun.policy.ExpBackoffPolicy;
import org.apache.falcon.rerun.queue.DelayedQueue;
import org.apache.log4j.Logger;

/**
 * Base class for a rerun consumer.
 *
 * @param <T> a rerun event
 * @param <M> a rerun handler
 */
public abstract class AbstractRerunConsumer<T extends RerunEvent, M extends AbstractRerunHandler<T, DelayedQueue<T>>>
        implements Runnable {

    protected static final Logger LOG = Logger
            .getLogger(AbstractRerunConsumer.class);

    protected M handler;

    public AbstractRerunConsumer(M handler) {
        this.handler = handler;
    }

    @Override
    public void run() {
        int attempt = 1;
        AbstractRerunPolicy policy = new ExpBackoffPolicy();
        Frequency frequency = new Frequency("minutes(1)");
        while (true) {
            try {
                T message = null;
                try {
                    message = handler.takeFromQueue();
                    attempt = 1;
                } catch (FalconException e) {
                    LOG.error("Error while reading message from the queue: ", e);
                    GenericAlert.alertRerunConsumerFailed(
                            "Error while reading message from the queue: ", e);
                    Thread.sleep(policy.getDelay(frequency, attempt));
                    handler.reconnect();
                    attempt++;
                    continue;
                }
                String jobStatus = handler.getWfEngine().getWorkflowStatus(
                        message.getClusterName(), message.getWfId());
                handleRerun(message.getClusterName(), jobStatus, message);

            } catch (Throwable e) {
                LOG.error("Error in rerun consumer:", e);
            }
        }

    }

    protected abstract void handleRerun(String cluster, String jobStatus, T message);
}
