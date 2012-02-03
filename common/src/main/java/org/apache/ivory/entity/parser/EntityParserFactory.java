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

package org.apache.ivory.entity.parser;

import org.apache.ivory.entity.v0.EntityType;

/**
 * 
 * Factory Class which returns the Parser based on the EntityType.
 * 
 */
public final class EntityParserFactory {

	private EntityParserFactory() {
	}

	/**
	 * Tie EnityType with the Entity Class in one place so that it can be
	 * unmarshalled easily by concrete classes based on the class type using
	 * JAXB.
	 * 
	 * @param entityType
	 * @return concrete parser based on entity type
	 */
	public static EntityParser<?> getParser(final EntityType entityType) {

		switch (entityType) {
		case PROCESS:
			return new ProcessEntityParser(entityType,
					org.apache.ivory.entity.v0.process.Process.class);
		case DATASET://TODO
			return new DatasetEntityParser(entityType,
					org.apache.ivory.entity.v0.dataset.Dataset.class);
		case CLUSTER:
			return new ClusterEntityParser(entityType,
					org.apache.ivory.entity.v0.cluster.Cluster.class);
		default:
			return null;
		}
	}

}
