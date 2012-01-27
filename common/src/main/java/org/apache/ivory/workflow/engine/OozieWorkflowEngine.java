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

package org.apache.ivory.workflow.engine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;

/**
 * Workflow engine which uses oozies APIs
 *
 */
public class OozieWorkflowEngine implements WorkflowEngine<Entity> {
	
	private static final Logger LOG = Logger.getLogger(OozieWorkflowEngine.class);

	public Configuration getDefaultConfiguration() {
		Configuration configuration = new Configuration(false);
		configuration.set(USER_NAME, StartupProperties.get().get(USER_NAME)
				.toString());
		configuration.set(NAME_NODE, StartupProperties.get().get(NAME_NODE)
				.toString());
		configuration.set(JOB_TRACKER,
				StartupProperties.get().get(JOB_TRACKER).toString());
		configuration.set(QUEUE_NAME, StartupProperties.get().get(QUEUE_NAME)
				.toString());

		return configuration;
	}

	public String schedule(Path path) throws IvoryException {
		HttpClient client = new HttpClient();
		String oozieUrl = StartupProperties.get().getProperty("oozie.url");
		PostMethod postMethod = new PostMethod(oozieUrl);

		Configuration configuration = getDefaultConfiguration();
		configuration.set("oozie.coord.application.path", path.toString());

		ByteArrayOutputStream out = new ByteArrayOutputStream(2048);
		try {
			configuration.writeXml(out);
			out.close();
			RequestEntity reqEntity = new ByteArrayRequestEntity(
					out.toByteArray(), "application/xml;charset=UTF-8");
			postMethod.setRequestEntity(reqEntity);
			client.executeMethod(postMethod);
			return postMethod.getResponseBodyAsString();
		} catch (IOException e) {
			LOG.error(e.getMessage());
			throw new IvoryException(e);
		}

	}

	@Override
	public String dryRun(Path path) throws IvoryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String suspend(String entityName) throws IvoryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String resume(String entityName) throws IvoryException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String delete(String entityName) throws IvoryException {
		// TODO Auto-generated method stub
		return null;
	}
}
