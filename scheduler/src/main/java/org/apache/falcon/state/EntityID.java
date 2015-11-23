/**
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

package org.apache.falcon.state;

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.datanucleus.util.StringUtils;

/**
 * A unique id for an entity(wrapped).
 */
public class EntityID extends ID {

    public EntityID(EntityType entityType, String entityName) {
        assert entityType != null : "Entity type must be present.";
        assert !StringUtils.isEmpty(entityName) : "Entity name must be present.";
        this.entityName = entityName;
        this.entityType = entityType;
        this.key = this.entityType + KEY_SEPARATOR + this.entityName;
    }

    public EntityID(Entity entity) {
        this(entity.getEntityType(), entity.getName());
    }

    @Override
    public String toString() {
        return key;
    }

    @Override
    public EntityID getEntityID() {
        return this;
    }
}
