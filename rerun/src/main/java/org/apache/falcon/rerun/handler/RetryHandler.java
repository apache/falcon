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
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.rerun.event.RetryEvent;
import org.apache.falcon.rerun.policy.AbstractRerunPolicy;
import org.apache.falcon.rerun.policy.RerunPolicyFactory;
import org.apache.falcon.rerun.queue.DelayedQueue;

/**
 * An implementation of retry handler that kicks off retries until the
 * configured attempts are exhausted.
 *
 * @param <M>
 */
public class RetryHandler<M extends DelayedQueue<RetryEvent>> extends
        AbstractRerunHandler<RetryEvent, M> {

    @Override
    public void handleRerun(String cluster, String entityType, String entityName,
                            String nominalTime, String runId, String wfId, long msgReceivedTime) {
        try {
            Entity entity = getEntity(entityType, entityName);
            Retry retry = getRetry(entity);

            if (retry == null) {
                LOG.warn("Retry not configured for entity:" + entityType + "("
                        + entity.getName() + "), ignoring failed retries");
                return;
            }

            int attempts = retry.getAttempts();
            Frequency delay = retry.getDelay();
            PolicyType policy = retry.getPolicy();
            int intRunId = Integer.parseInt(runId);

            if (attempts > intRunId) {
                AbstractRerunPolicy rerunPolicy = RerunPolicyFactory.getRetryPolicy(policy);
                long delayTime = rerunPolicy.getDelay(delay, Integer.parseInt(runId));
                RetryEvent event = new RetryEvent(cluster, wfId,
                        msgReceivedTime, delayTime, entityType, entityName,
                        nominalTime, intRunId, attempts, 0);
                offerToQueue(event);
            } else {
                LOG.warn("All retry attempt failed out of configured: "
                        + attempts + " attempt for entity instance::"
                        + entityName + ":" + nominalTime + " And WorkflowId: "
                        + wfId);

                GenericAlert.alertRetryFailed(entityType, entityName,
                        nominalTime, wfId, runId,
                        "All retry attempt failed out of configured: "
                                + attempts + " attempt for entity instance::");
            }
        } catch (FalconException e) {
            LOG.error("Error during retry of entity instance " + entityName + ":" + nominalTime, e);
            GenericAlert.alertRetryFailed(entityType, entityName, nominalTime, wfId, runId, e.getMessage());
        }
    }

    @Override
    public void init(M aDelayQueue) throws FalconException {
        super.init(aDelayQueue);
        Thread daemon = new Thread(new RetryConsumer(this));
        daemon.setName("RetryHandler");
        daemon.setDaemon(true);
        daemon.start();
        LOG.info("RetryHandler thread started.");
    }
}
