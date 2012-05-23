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
package org.apache.ivory.rerun.event;

import org.apache.ivory.rerun.event.RerunEvent.RerunType;
import org.apache.ivory.workflow.engine.WorkflowEngine;

public class RerunEventFactory<T extends RerunEvent> {

	public T getRerunEvent(String type, WorkflowEngine wfEngine, String line) {
		if (type.startsWith(RerunType.RETRY.name())) {
			return retryEventFromString(wfEngine, line);
		} else if (type.startsWith(RerunType.LATE.name())) {
			return lateEventFromString(wfEngine, line);
		} else
			return null;
	}

	private T lateEventFromString(WorkflowEngine wfEngine, String line) {
		// TODO Auto-generated method stub
		return null;
	}

	public T retryEventFromString(WorkflowEngine workflowEngine, String message) {
		String[] items = message.split("\\" + RerunEvent.SEP);
		T event = (T) new RetryEvent(workflowEngine, items[0], items[1],
				Long.parseLong(items[2]), Long.parseLong(items[3]), items[4],
				items[5], Integer.parseInt(items[6]),
				Integer.parseInt(items[7]), Integer.parseInt(items[8]));
		return event;
	}
}
