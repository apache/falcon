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
import org.apache.ivory.rerun.event.RerunEvent;
import org.apache.ivory.rerun.queue.DelayedQueue;
import org.apache.log4j.Logger;

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
		while (true) {
			try {
				T message = null;
				try {
					message = handler.takeFromQueue();
				} catch (IvoryException e) {
					LOG.error("Error while reading message from the queue: ", e);
					continue;
				}
				String jobStatus = handler.getWfEngine().getWorkflowStatus(
						message.getClusterName(), message.getWfId());
				handleRerun(message.getClusterName(),jobStatus, message);

			} catch (Throwable e) {
				LOG.error("Error in rerun consumer:", e);
			}
		}

	}

	protected abstract void handleRerun(String cluster, String jobStatus, T message);
}
