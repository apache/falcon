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

package org.apache.ivory.entity.v0;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.reflection.FieldDictionary;
import com.thoughtworks.xstream.converters.reflection.ImmutableFieldKeySorter;
import com.thoughtworks.xstream.converters.reflection.Sun14ReflectionProvider;
import com.thoughtworks.xstream.io.xml.DomDriver;
import org.apache.ivory.Tag;

public abstract class Entity {
    private static final String PREFIX = "IVORY_";

    public abstract String getName();

    public EntityType getEntityType() {
        for (EntityType type : EntityType.values()) {
            if (type.getEntityClass().equals(getClass())) {
                return type;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!o.getClass().equals(this.getClass()))
            return false;

        Entity entity = (Entity) o;

        String name = getName();
        if (name != null ? !name.equals(entity.getName()) : entity.getName() != null)
            return false;

        return true;
    }

    public boolean deepEquals(Entity entity) {
        if (entity == null)
            return false;
        if (this == entity)
            return true;
        if (!equals(entity))
            return false;

        XStream xstream = new XStream(new Sun14ReflectionProvider(new FieldDictionary(new ImmutableFieldKeySorter())), new DomDriver(
                "utf-8"));
        String thisStr = xstream.toXML(this);
        String entityStr = xstream.toXML(entity);
        return thisStr.equals(entityStr);
    }

    @Override
    public int hashCode() {
        String clazz = this.getClass().getName();

        String name = getName();
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + clazz.hashCode();
        return result;
    }

    @Override
    public String toString() {
        try {
            StringWriter stringWriter = new StringWriter();
            Marshaller marshaller = getEntityType().getMarshaller();
            marshaller.marshal(this, stringWriter);
            return stringWriter.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Entity fromString(EntityType type, String str) {
        try {
            Unmarshaller unmarshaler = type.getUnmarshaller();
            return (Entity) unmarshaler.unmarshal(new StringReader(str));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String toShortString() {
        return "(" + getEntityType().name().toLowerCase() + ") " + getName();
    }

    @Override
    public Entity clone() {
        return fromString(getEntityType(), toString());
    }

    public abstract String[] getClustersDefined();
}
