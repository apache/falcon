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

package org.apache.falcon.resource;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Instance list of an extension job used for marshalling / unmarshalling with REST calls.
 */
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ExtensionInstanceList {
    @XmlElement
    private int numEntities;

    @XmlElementWrapper(name = "entitiesSummary")
    private List<EntitySummary> entitySummary;

    public ExtensionInstanceList() {
        numEntities = 0;
        entitySummary = null;
    }

    public ExtensionInstanceList(int numEntities) {
        this.numEntities = numEntities;
        entitySummary = new ArrayList<>();
    }

    public ExtensionInstanceList(int numEntities, List<EntitySummary> entitySummary) {
        this.numEntities = numEntities;
        this.entitySummary = entitySummary;
    }

    public void addEntitySummary(EntitySummary summary) {
        entitySummary.add(summary);
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(numEntities + "\n\n");
        for (EntitySummary summary : entitySummary) {
            buffer.append(summary.toString());
        }
        return buffer.toString();
    }

    /**
     * Summary of an entity (including entity properties and instances.
     */
    public static class EntitySummary {
        @XmlElement
        private EntityList.EntityElement entityProfile;

        @XmlElement
        private InstancesResult.Instance[] instances;

        public EntitySummary() {
            entityProfile = null;
            instances = null;
        }

        public EntitySummary(EntityList.EntityElement entityProfile, InstancesResult.Instance[] instances) {
            this.entityProfile = entityProfile;
            this.instances = instances;
        }

        public String toString() {
            StringBuilder buffer = new StringBuilder();
            buffer.append(entityProfile.toString() + "\n");
            buffer.append(Arrays.toString(instances) + "\n");
            return buffer.toString();
        }
    }
}
