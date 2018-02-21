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
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.rerun.event.RerunEvent;
import org.apache.falcon.rerun.queue.DelayedQueue;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.HostnameFilter;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for handling reruns.
 *
 * @param <T> a rerun event
 * @param <M> queue
 */
public abstract class AbstractRerunHandler<T extends RerunEvent, M extends DelayedQueue<T>>
        implements WorkflowExecutionListener {

    protected static final Logger LOG = LoggerFactory.getLogger(LateRerunHandler.class);
    protected M delayQueue;
    private AbstractWorkflowEngine wfEngine;

    public void init(M aDelayQueue) throws FalconException {
        this.wfEngine = WorkflowEngineFactory.getWorkflowEngine();
        this.delayQueue = aDelayQueue;
        this.delayQueue.init();
    }

    public void close() throws FalconException {
        this.delayQueue.close();
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public abstract void handleRerun(String clusterName, String entityType,
                                     String entityName, String nominalTime, String runId,
                                     String wfId, String parentId, String workflowUser, long msgReceivedTime);
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    public AbstractWorkflowEngine getWfEngine(String entityType, String entityName, String doAsUser)
        throws FalconException {
        if (StringUtils.isNotBlank(doAsUser)) {
            CurrentUser.authenticate(doAsUser);
            CurrentUser.proxyDoAsUser(doAsUser, HostnameFilter.get());
        }
        if (StringUtils.isBlank(entityType) || StringUtils.isBlank(entityName)) {
            return wfEngine;
        }
        Entity entity = EntityUtil.getEntity(EntityType.valueOf(entityType), entityName);
        return WorkflowEngineFactory.getWorkflowEngine(entity);

    }

    public boolean offerToQueue(T event) throws FalconException {
        return delayQueue.offer(event);
    }

    public T takeFromQueue() throws FalconException {
        return delayQueue.take();
    }

    public void reconnect() throws FalconException {
        delayQueue.reconnect();
    }

    public Retry getRetry(Entity entity) throws FalconException {
        return EntityUtil.getRetry(entity);
    }
}
