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

import org.apache.falcon.entity.v0.Entity;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

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
        //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

        @Override
        public String toString() {
            return "(" + type + ") " + name + "(" + status + ")\n";
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
            Entity e = elements[i];
            EntityElement o = new EntityElement();
            o.type = e.getEntityType().name().toLowerCase();
            o.name = e.getName();
            o.status = "";
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
            buffer.append(element);
        }
        return buffer.toString();
    }
}
