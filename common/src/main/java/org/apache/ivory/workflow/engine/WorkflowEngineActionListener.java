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

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;

public interface WorkflowEngineActionListener {

    void beforeSchedule(Cluster cluster, Entity entity) throws IvoryException;

    void afterSchedule(Cluster cluster, Entity entity) throws IvoryException;

    void beforeDelete(Cluster cluster, Entity entity) throws IvoryException;

    void afterDelete(Cluster cluster, Entity entity) throws IvoryException;

    void beforeSuspend(Cluster cluster, Entity entity) throws IvoryException;

    void afterSuspend(Cluster cluster, Entity entity) throws IvoryException;

    void beforeResume(Cluster cluster, Entity entity) throws IvoryException;

    void afterResume(Cluster cluster, Entity entity) throws IvoryException;
}
