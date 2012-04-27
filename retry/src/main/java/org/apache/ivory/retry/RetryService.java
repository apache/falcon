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
package org.apache.ivory.retry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.service.IvoryService;
import org.apache.ivory.workflow.WorkflowEngineFactory;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

public class RetryService implements IvoryService {

	private static final Logger LOG = Logger.getLogger(RetryService.class);

	@Override
	public String getName() {
		return "Ivory Retry failed Instance";
	}

	@Override
	public void init() throws IvoryException {
		Thread daemon = new RetryHandler.Consumer();
		daemon.setName("RetryHandler");
		daemon.setDaemon(true);
		daemon.start();
		LOG.info("RetryHandler  thread started");
		RetryHandler.setBasePath();
		bootstrap();
	}

	private void bootstrap() {
		WorkflowEngine workflowEngine;
		try {
			workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
		} catch (IvoryException e) {
			throw new IvoryRuntimException(e);
		}
		for (File retryFile : RetryHandler.getBasePath().listFiles()) {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(
						retryFile));
				String line;
				while ((line = reader.readLine()) != null) {
					RetryEvent event = RetryEvent.fromString(workflowEngine,
							line);
					RetryHandler.enqueue(event);
				}
			} catch (Exception e) {
				LOG.warn(
						"Not able to read retry entry "
								+ retryFile.getAbsolutePath(), e);
			}
		}

	}

	@Override
	public void destroy() throws IvoryException {
		LOG.info("RetryHandler  thread destroyed");

	}

}
