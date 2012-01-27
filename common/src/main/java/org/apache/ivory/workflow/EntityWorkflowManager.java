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

package org.apache.ivory.workflow;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.ivory.workflow.engine.WorkflowEngine;

/**
 * 
 * 
 * @param <T>- Process, Dataset or Datastore, Implementations are provided by
 *        concrete entity workflow manager.
 */
public abstract class EntityWorkflowManager<T extends Entity> {

	private WorkflowEngine<Entity> workflowEngine = new OozieWorkflowEngine();

	/**
	 * Default workflow engine is set to oozie, clients may override this.
	 * 
	 * @param workflowEngine
	 */
	public final void setWorkflowEngine(WorkflowEngine<Entity> workflowEngine) {
		this.workflowEngine = workflowEngine;
	}

	public final WorkflowEngine<Entity> getWorkflowEngine() {
		return this.workflowEngine;
	}

	public abstract String schedule(T entity) throws IvoryException;

	public abstract String dryRun(T entity) throws IvoryException;

	public abstract String suspend(String entityName) throws IvoryException;

	public abstract String resume(String entityName) throws IvoryException;

	public abstract String delete(String entityName) throws IvoryException;
}
