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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Entity list used for marshalling / unmarshalling with REST calls.
 */
@XmlRootElement(name = "entities")
@XmlAccessorType(XmlAccessType.FIELD)
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class EntityList {

    @XmlElement(name = "entity")
    private final EntityElement[] elements;

    /**
     * List of fields returned by RestAPI.
     */
    public static enum EntityFieldList {
        TYPE, NAME, STATUS, TAGS, PIPELINES
    }

    /**
     * Filter by these Fields is supported by RestAPI.
     */
    public static enum EntityFilterByFields {
        TYPE, NAME, STATUS, PIPELINES, CLUSTER
    }

    /**
     * Element within an entity.
     */
    public static class EntityElement {
        //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
        @XmlElement
        public String type;
        @XmlElement
        public String name;
        @XmlElement
        public String status;
        @XmlElementWrapper(name = "tags")
        public List<String> tag;
        @XmlElementWrapper(name = "pipelines")
        public List<String> pipeline;
        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

        @Override
        public String toString() {
            String outString = "(" + type + ") " + name;
            if (StringUtils.isNotEmpty(status)) {
                outString += "(" + status + ")";
            }

            if (tag != null && !tag.isEmpty()) {
                outString += " - " + tag.toString();
            }

            if (pipeline != null && !pipeline.isEmpty()) {
                outString += " - " + pipeline.toString();
            }
            outString += "\n";
            return outString;
        }
    }

    //For JAXB
    public EntityList() {
        this.elements = null;
    }

    public EntityList(EntityElement[] elements) {
        this.elements = elements;
    }

    public EntityList(Entity[] elements) {
        int len = elements.length;
        EntityElement[] items = new EntityElement[len];
        for (int i = 0; i < len; i++) {
            items[i] = createEntityElement(elements[i]);
        }
        this.elements = items;
    }

    private EntityElement createEntityElement(Entity e) {
        EntityElement element = new EntityElement();
        element.type = e.getEntityType().name().toLowerCase();
        element.name = e.getName();
        element.status = null;
        element.tag = new ArrayList<String>();
        element.pipeline = new ArrayList<String>();
        return element;
    }

    public EntityList(Entity[] dependentEntities, Entity entity) {
        int len = dependentEntities.length;
        EntityElement[] items = new EntityElement[len];
        for (int i = 0; i < len; i++) {
            Entity e = dependentEntities[i];
            EntityElement o = new EntityElement();
            o.type = e.getEntityType().name().toLowerCase();
            o.name = e.getName();
            o.status = null;
            o.tag = getEntityTag(e, entity);
            items[i] = o;
        }
        this.elements = items;
    }

    public EntityElement[] getElements() {
        return elements;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        for (EntityElement element : elements) {
            buffer.append(element.toString());
        }
        return buffer.toString();
    }

    private List<String> getEntityTag(Entity dependentEntity, Entity entity) {
        List<String> tagList = new ArrayList<String>();

        if (entity.getEntityType().equals(EntityType.CLUSTER)) {
            return tagList;
        }

        Process process = null;
        String entityNameToMatch = null;
        if (dependentEntity.getEntityType().equals(EntityType.PROCESS)) {
            process = (Process) dependentEntity;
            entityNameToMatch = entity.getName();
        } else if (dependentEntity.getEntityType().equals(EntityType.FEED)
                && entity.getEntityType().equals(EntityType.PROCESS)) {
            process = (Process) entity;
            entityNameToMatch = dependentEntity.getName();
        }

        if (process != null) {
            if (process.getInputs() != null) {
                for (Input i : process.getInputs().getInputs()) {
                    if (i.getFeed().equals(entityNameToMatch)) {
                        tagList.add("Input");
                    }
                }
            }
            if (process.getOutputs() != null) {
                for (Output o : process.getOutputs().getOutputs()) {
                    if (o.getFeed().equals(entityNameToMatch)) {
                        tagList.add("Output");
                    }
                }
            }
        }

        return tagList;
    }
}
