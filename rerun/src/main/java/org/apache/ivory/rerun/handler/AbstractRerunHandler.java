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
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.rerun.event.RerunEvent;
import org.apache.ivory.rerun.queue.DelayedQueue;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

public abstract class AbstractRerunHandler<T extends RerunEvent, M extends DelayedQueue<T>> {

	protected static final Logger LOG = Logger
			.getLogger(LateRerunHandler.class);
	protected M delayQueue;

	public void init(M delayQueue) throws IvoryException {
		this.delayQueue = delayQueue;
		this.delayQueue.init();
	}

	public abstract void handleRerun(String processName, String nominalTime,
			String runId, String wfId, WorkflowEngine wfEngine,
			long msgReceivedTime) throws IvoryException;

	public boolean offerToQueue(T event) {
		return delayQueue.offer(event);
	}

	public T takeFromQueue() throws IvoryException {
		return delayQueue.take();
	}

	public Process getProcess(String processName) throws IvoryException {
		return ConfigurationStore.get().get(EntityType.PROCESS, processName);
	}

	protected boolean validate(String processName, Process processObj) {
		if (processObj == null) {
			LOG.warn("Ignoring rerun, as the process:" + processName
					+ " does not exists in config store");
			return false;
		}
		return true;
	}

}
