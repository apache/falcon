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
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.LateInput;
import org.apache.falcon.entity.v0.process.LateProcess;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.rerun.event.LaterunEvent;
import org.apache.falcon.rerun.policy.AbstractRerunPolicy;
import org.apache.falcon.rerun.policy.RerunPolicyFactory;
import org.apache.falcon.rerun.queue.DelayedQueue;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Date;

/**
 * An implementation of handler for late reruns.
 *
 * @param <M>
 */
public class LateRerunHandler<M extends DelayedQueue<LaterunEvent>> extends
        AbstractRerunHandler<LaterunEvent, M> {
    private Thread daemon;

    @Override
    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public void handleRerun(String cluster, String entityType, String entityName, String nominalTime,
                            String runId, String wfId, String parentId, String workflowUser, long msgReceivedTime) {
        try {
            Entity entity = EntityUtil.getEntity(entityType, entityName);
            int intRunId = Integer.parseInt(runId);
            Date msgInsertTime = EntityUtil.parseDateUTC(nominalTime);
            Long wait = getEventDelay(entity, nominalTime);
            if (wait == -1) {
                LOG.info("Late rerun expired for entity: {} ({})", entityType, entityName);
                AbstractWorkflowEngine wfEngine = this.getWfEngine(entityType, entityName, entity.getACL().getOwner());
                java.util.Properties properties = wfEngine.getWorkflowProperties(cluster, wfId);
                String logDir = properties.getProperty("logDir");
                String srcClusterName = properties.getProperty("srcClusterName");
                Path lateLogPath = this.getLateLogPath(logDir,
                        EntityUtil.fromUTCtoURIDate(nominalTime), srcClusterName);

                LOG.info("Going to delete path: {}", lateLogPath);
                final String storageEndpoint = properties.getProperty(AbstractWorkflowEngine.NAME_NODE);
                Configuration conf = getConfiguration(storageEndpoint);
                FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);
                if (fs.exists(lateLogPath)) {
                    boolean deleted = fs.delete(lateLogPath, true);
                    if (deleted) {
                        LOG.info("Successfully deleted late file path: {}", lateLogPath);
                    }
                }
                return;
            }

            LOG.debug("Scheduling the late rerun for entity instance: {} ({}): {} And WorkflowId: {}",
                    entityType, entityName, nominalTime, wfId);
            LaterunEvent event = new LaterunEvent(cluster, wfId, parentId, msgInsertTime.getTime(),
                    wait, entityType, entityName, nominalTime, intRunId, workflowUser);
            offerToQueue(event);
        } catch (Exception e) {
            if (e instanceof EntityNotRegisteredException) {
                LOG.warn("Entity {} of type {} doesn't exist in config store. So late rerun "
                                + "cannot be done for workflow ", entityName,
                        entityType, wfId);
                return;
            }
            LOG.error("Unable to schedule late rerun for entity instance: {} ({}): {} And WorkflowId: {}",
                    entityType, entityName, nominalTime, wfId, e);
            GenericAlert.alertLateRerunFailed(entityType, entityName,
                    nominalTime, wfId, workflowUser, runId, e.getMessage());
        }
    }
    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck

    private long getEventDelay(Entity entity, String nominalTime) throws FalconException {

        Date instanceDate = EntityUtil.parseDateUTC(nominalTime);
        LateProcess lateProcess = EntityUtil.getLateProcess(entity);
        if (lateProcess == null) {
            LOG.warn("Late run not applicable for entity: {} ({})", entity.getEntityType(), entity.getName());
            return -1;
        }
        PolicyType latePolicy = lateProcess.getPolicy();
        Date cutOffTime = getCutOffTime(entity, nominalTime);
        Date now = new Date();
        Long wait;

        if (now.after(cutOffTime)) {
            LOG.warn("Feed Cut Off time: {} has expired, Late Rerun can not be scheduled",
                    SchemaHelper.formatDateUTC(cutOffTime));
            return -1;
        } else {
            AbstractRerunPolicy rerunPolicy = RerunPolicyFactory
                    .getRetryPolicy(latePolicy);
            wait = rerunPolicy.getDelay(lateProcess.getDelay(), instanceDate,
                    cutOffTime);
        }
        return wait;
    }

    public static Date addTime(Date date, long milliSecondsToAdd) {
        return new Date(date.getTime() + milliSecondsToAdd);
    }

    public static Date getCutOffTime(Entity entity, String nominalTime) throws FalconException {
        ExpressionHelper evaluator = ExpressionHelper.get();
        Date instanceStart = EntityUtil.parseDateUTC(nominalTime);
        ExpressionHelper.setReferenceDate(instanceStart);
        Date endTime;
        Date feedCutOff = new Date(0);
        if (entity.getEntityType() == EntityType.FEED) {
            if (((Feed) entity).getLateArrival() == null) {
                LOG.debug("Feed's {} late arrival cut-off is not configured, returning", entity.getName());
                return feedCutOff;
            }
            String lateCutOff = ((Feed) entity).getLateArrival().getCutOff()
                    .toString();
            endTime = EntityUtil.parseDateUTC(nominalTime);
            long feedCutOffPeriod = evaluator.evaluate(lateCutOff, Long.class);
            endTime = addTime(endTime, feedCutOffPeriod);
            return endTime;
        } else if (entity.getEntityType() == EntityType.PROCESS) {
            Process process = (Process) entity;
            ConfigurationStore store = ConfigurationStore.get();
            for (LateInput lp : process.getLateProcess().getLateInputs()) {
                Feed feed = null;
                String endInstanceTime = "";
                if (process.getInputs() != null) {
                    for (Input input : process.getInputs().getInputs()) {
                        if (input.getName().equals(lp.getInput())) {
                            endInstanceTime = input.getEnd();
                            feed = store.get(EntityType.FEED, input.getFeed());
                            break;
                        }
                    }
                }
                if (feed == null) {
                    throw new IllegalStateException("No such feed: " + lp.getInput());
                }
                if (feed.getLateArrival() == null) {
                    LOG.debug("Feed's {} late arrival cut-off is not configured, ignoring this feed", feed.getName());
                    continue;
                }
                String lateCutOff = feed.getLateArrival().getCutOff()
                        .toString();
                endTime = evaluator.evaluate(endInstanceTime, Date.class);
                long feedCutOffPeriod = evaluator.evaluate(lateCutOff,
                        Long.class);
                endTime = addTime(endTime, feedCutOffPeriod);

                if (endTime.after(feedCutOff)) {
                    feedCutOff = endTime;
                }
            }
            return feedCutOff;
        } else {
            throw new FalconException("Invalid entity while getting cut-off time:" + entity.getName());
        }
    }

    @Override
    public void init(M aDelayQueue) throws FalconException {
        super.init(aDelayQueue);
        daemon = new Thread(new LateRerunConsumer(this));
        daemon.setName("LaterunHandler");
        daemon.setDaemon(true);
        daemon.start();
        LOG.info("Laterun Handler thread started");
    }

    @Override
    public void close() throws FalconException {
        daemon.interrupt();
        super.close();
    }


    public Path getLateLogPath(String logDir, String nominalTime,
                               String srcClusterName) {
        //SrcClusterName valid only in case of feed
        return new Path(logDir + "/latedata/" + nominalTime + "/"
                + (srcClusterName == null
                ? "" : srcClusterName));

    }

    public static Configuration getConfiguration(String storageEndpoint) throws FalconException {
        Configuration conf = new Configuration();
        conf.set(HadoopClientFactory.FS_DEFAULT_NAME_KEY, storageEndpoint);
        return conf;
    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {
        Entity entity = EntityUtil.getEntity(context.getEntityType(), context.getEntityName());
        //late data handling not applicable for feed retention action
        if (context.getOperation() != WorkflowExecutionContext.EntityOperations.DELETE
                && EntityUtil.getLateProcess(entity) != null) {
            handleRerun(context.getClusterName(), context.getEntityType(),
                    context.getEntityName(), context.getNominalTimeAsISO8601(),
                    context.getWorkflowRunIdString(), context.getWorkflowId(), context.getWorkflowParentId(),
                    context.getWorkflowUser(), context.getExecutionCompletionTime());
        } else {
            LOG.info("Late date handling not applicable for entityType: " + context.getEntityType()
                    + ", entityName: " + context.getEntityName()
                    + " operation: " + context.getOperation());
        }
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        // do nothing since late data does not apply for failed workflows
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException {
        // do nothing
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException {
        // do nothing
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException {
        // Do nothing.
    }
}
