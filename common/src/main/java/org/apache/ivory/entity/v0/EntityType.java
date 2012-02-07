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

package org.apache.ivory.entity.v0;

import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;

/**
 * Enum for types of entities in Ivory
 * Process, Feed and Cluster
 */
public enum EntityType {
	FEED(Feed.class), PROCESS(Process.class), CLUSTER(Cluster.class);

	private final Class<? extends Entity> clazz;

	EntityType(Class<? extends Entity> typeClass) {
		clazz = typeClass;
	}

	public Class<? extends Entity> getEntityClass() {
		return clazz;
	}

	public boolean isSchedulable() {
		return this.equals(EntityType.CLUSTER) ? false : true;
	}
}
