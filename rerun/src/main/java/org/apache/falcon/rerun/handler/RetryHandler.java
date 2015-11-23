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
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.rerun.event.RetryEvent;
import org.apache.falcon.rerun.policy.AbstractRerunPolicy;
import org.apache.falcon.rerun.policy.RerunPolicyFactory;
import org.apache.falcon.rerun.queue.DelayedQueue;
import org.apache.falcon.workflow.WorkflowExecutionContext;

/**
 * An implementation of retry handler that kicks off retries until the
 * configured attempts are exhausted.
 *
 * @param <M>
 */
public class RetryHandler<M extends DelayedQueue<RetryEvent>> extends
        AbstractRerunHandler<RetryEvent, M> {
    private Thread daemon;

    @Override
    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public void handleRerun(String clusterName, String entityType, String entityName, String nominalTime,
                            String runId, String wfId, String workflowUser, long msgReceivedTime) {
        try {
            Entity entity = EntityUtil.getEntity(entityType, entityName);
            Retry retry = getRetry(entity);

            if (retry == null) {
                LOG.warn("Retry not configured for entity: {} ({}), ignoring failed retried",
                        entityType, entity.getName());
                return;
            }

            int attempts = retry.getAttempts();
            Frequency delay = retry.getDelay();
            PolicyType policy = retry.getPolicy();
            int intRunId = Integer.parseInt(runId);

            if (attempts > intRunId) {
                AbstractRerunPolicy rerunPolicy = RerunPolicyFactory.getRetryPolicy(policy);
                long delayTime = rerunPolicy.getDelay(delay, Integer.parseInt(runId));
                RetryEvent event = new RetryEvent(clusterName, wfId,
                        msgReceivedTime, delayTime, entityType, entityName,
                        nominalTime, intRunId, attempts, 0, workflowUser);
                offerToQueue(event);
            } else {
                LOG.warn("All retry attempt failed out of configured: {} attempt for entity instance: {}:{} "
                    + "And WorkflowId: {}", attempts, entityName, nominalTime, wfId);

                GenericAlert.alertRetryFailed(entityType, entityName,
                        nominalTime, wfId, workflowUser, runId,
                        "All retry attempt failed out of configured: "
                                + attempts + " attempt for entity instance::");
            }
        } catch (FalconException e) {
            LOG.error("Error during retry of entity instance {}:{}", entityName, nominalTime, e);
            GenericAlert.alertRetryFailed(entityType, entityName, nominalTime,
                    wfId, workflowUser, runId, e.getMessage());
        }
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    @Override
    public void init(M aDelayQueue) throws FalconException {
        super.init(aDelayQueue);
        daemon = new Thread(new RetryConsumer(this));
        daemon.setName("RetryHandler");
        daemon.setDaemon(true);
        daemon.start();
        LOG.info("RetryHandler thread started.");
    }

    @Override
    public void close() throws FalconException {
        daemon.interrupt();
        super.close();
    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {
        // do nothing since retry does not apply for failed workflows
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        // Re-run does not make sense on timeouts or when killed by user.
        if (context.hasWorkflowTimedOut() || context.isWorkflowKilledManually()) {
            return;
        }
        handleRerun(context.getClusterName(), context.getEntityType(),
                context.getEntityName(), context.getNominalTimeAsISO8601(),
                context.getWorkflowRunIdString(), context.getWorkflowId(),
                context.getWorkflowUser(), context.getExecutionCompletionTime());
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException {
        // Do nothing
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException {
        // Do nothing
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException {
        // Do nothing
    }
}
