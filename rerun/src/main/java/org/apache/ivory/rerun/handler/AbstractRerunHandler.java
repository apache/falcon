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
package org.apache.ivory.rerun.handler;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.process.Retry;
import org.apache.ivory.rerun.event.RerunEvent;
import org.apache.ivory.rerun.queue.DelayedQueue;
import org.apache.ivory.workflow.WorkflowEngineFactory;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

public abstract class AbstractRerunHandler<T extends RerunEvent, M extends DelayedQueue<T>> {

	protected static final Logger LOG = Logger
			.getLogger(LateRerunHandler.class);
	protected M delayQueue;
	private WorkflowEngine wfEngine;

	public void init(M delayQueue) throws IvoryException {
		this.wfEngine = WorkflowEngineFactory.getWorkflowEngine();
		this.delayQueue = delayQueue;
		this.delayQueue.init();
	}

	public abstract void handleRerun(String cluster, String entityType,
			String entityName, String nominalTime, String runId, String wfId,
			long msgReceivedTime);

	public WorkflowEngine getWfEngine() {
		return wfEngine;
	}

	public boolean offerToQueue(T event) {
		return delayQueue.offer(event);
	}

	public T takeFromQueue() throws IvoryException {
		return delayQueue.take();
	}

	public Entity getEntity(String entityType, String entityName)
			throws IvoryException {
		return EntityUtil.getEntity(entityType, entityName);
	}

	public Retry getRetry(Entity entity) throws IvoryException {
		return EntityUtil.getRetry(entity);
	}

}
