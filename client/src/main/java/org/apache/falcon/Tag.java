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

package org.apache.falcon;

import org.apache.falcon.entity.v0.EntityType;

public enum Tag {
    DEFAULT(EntityType.PROCESS), RETENTION(EntityType.FEED), REPLICATION(EntityType.FEED);

    private final EntityType entityType;

    private Tag(EntityType entityType) {
        this.entityType = entityType;
    }

    public EntityType getType() {
        return entityType;
    }
}
