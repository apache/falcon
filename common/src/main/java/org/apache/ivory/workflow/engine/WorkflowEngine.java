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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;

/**
 * 
 * @param <T> -
 *            Process, Dataset or Datastore which extends entity Interface which
 *            all the workflow engine like oozie or azkaban should extend
 */
public interface WorkflowEngine<T extends Entity> {

	String USER_NAME = "user.name";
	String GROUP_NAME = "group.name";
	String NAME_NODE = "nameNode";
	String JOB_TRACKER = "jobTracker";
	String QUEUE_NAME = "queueName";
	
	String URI_SEPERATOR="/";

	Configuration getDefaultConfiguration();

	String schedule(Path path) throws IvoryException;

	String dryRun(Path path) throws IvoryException;

	String suspend(String entityName) throws IvoryException;

	String resume(String entityName) throws IvoryException;

	String delete(String entityName) throws IvoryException;

}
