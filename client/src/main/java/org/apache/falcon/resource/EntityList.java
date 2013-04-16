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
public class EntityList {

    @XmlElement(name = "entity")
    private EntityElement[] elements;

    /**
     * Element within an entity.
     */
    public static class EntityElement {
        @XmlElement
        public String type;
        @XmlElement
        public String name;

        public EntityElement() {

        }

        public EntityElement(String type, String name) {
            this.type = type;
            this.name = name;
        }

        @Override
        public String toString() {
            return "(" + type + ") " + name + "\n";
        }
    }

    //For JAXB
    public EntityList() {
    }

    public EntityList(Entity[] elements) {
        EntityElement[] items = new EntityElement[elements.length];
        for (int index = 0; index < elements.length; index++) {
            items[index] = new EntityElement(elements[index].
                    getEntityType().name().toLowerCase(), elements[index].getName());
        }
        this.elements = items;
    }

    public EntityElement[] getElements() {
        return elements;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        for (EntityElement element : elements) {
            buffer.append(element);
        }
        return buffer.toString();
    }
}
