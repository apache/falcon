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

import org.apache.ivory.IvoryException;
import org.apache.ivory.retry.RetryHandler.Consumer;
import org.apache.ivory.service.IvoryService;
import org.apache.log4j.Logger;

public class RetryService implements IvoryService{

	private static final Logger LOG = Logger
			.getLogger(RetryService.class);
	
	@Override
	public String getName() {
		return "Ivory Retry failed Instance";
	}

	@Override
	public void init() throws IvoryException {
			Thread daemon = new Consumer();
			daemon.setName("RetryHandler");
			daemon.setDaemon(true);
			daemon.start();
			LOG.info("RetryHandler  thread started");
	}

	@Override
	public void destroy() throws IvoryException {
		LOG.info("RetryHandler  thread destroyed");
		
	}

}
