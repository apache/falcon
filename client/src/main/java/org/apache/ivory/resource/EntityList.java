package org.apache.ivory.resource;

import org.apache.ivory.entity.v0.Entity;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * User: sriksun
 * Date: 19/03/12
 */
@XmlRootElement(name = "entities")
@XmlAccessorType(XmlAccessType.FIELD)
public class EntityList {

    @XmlElement (name = "entity")
    private EntityElement[] elements;

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
    public EntityList() {}

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
