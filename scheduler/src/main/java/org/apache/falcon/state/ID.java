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

import org.apache.falcon.entity.v0.EntityType;

import java.io.Serializable;

/**
 * A serializable, comparable ID used to uniquely represent an entity or an instance.
 */
public abstract class ID implements Serializable, Comparable<ID> {
    public static final String KEY_SEPARATOR = "/";
    protected String entityName;
    protected EntityType entityType;
    protected String key;

    @Override
    public boolean equals(Object id) {
        if (id == null || id.getClass() != getClass()) {
            return false;
        }
        return compareTo((ID) id) == 0;
    }

    @Override
    public String toString() {
        return key;
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public int compareTo(ID id) {
        if (id == null) {
            return -1;
        }
        return this.toString().compareTo(id.toString());
    }

    public String getEntityName() {
        return entityName;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public String getKey() {
        return key;
    }

    public abstract EntityID getEntityID();

}
