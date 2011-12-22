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
package org.apache.airavat.mappers;

import org.apache.airavat.entity.v0.Entity;
import org.apache.airavat.oozie.coordinator.COORDINATORAPP;

/**
 * Pass a partially filled coordinatorapp object project with the entity
 */
public class CoordinatorMapper implements CustomMapper {

	private final COORDINATORAPP coordinatorapp;

	private final Entity entity;

	/**
	 * Pass a ProcessType Object and partially filled coordinator Object
	 * 
	 * @param coordinatorapp
	 * @param entity
	 */
	public CoordinatorMapper(final Entity entity,
			final COORDINATORAPP coordinatorapp) {
		super();
		this.coordinatorapp = coordinatorapp;
		this.entity = entity;
	}

	@Override
	public void map() {
		DozerProvider.map(new String[] { "process-to-coordinator.xml" },
				this.entity,
				this.coordinatorapp);

		// Map custom fields
		DozerProvider.map(new String[] { "custom-coordinator.xml" },
				this.entity, this.coordinatorapp);

	}

	@Override
	public Entity getEntity() {
		return this.entity;
	}

	@Override
	public COORDINATORAPP getCoordinatorapp() {
		return this.coordinatorapp;
	}

}
