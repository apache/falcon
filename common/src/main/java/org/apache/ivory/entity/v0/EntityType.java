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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;

/**
 * Enum for types of entities in Ivory Process, Feed and Cluster
 */
public enum EntityType {
    FEED(Feed.class, "/schema/feed/feed-0.1.xsd"), 
    PROCESS(Process.class, "/schema/process/process-0.1.xsd"), 
    CLUSTER(Cluster.class, "/schema/cluster/cluster-0.1.xsd");

    private final Class<? extends Entity> clazz;
    private JAXBContext jaxbContext;
    private Schema schema;

    private EntityType(Class<? extends Entity> typeClass, String schemaFile) {
        clazz = typeClass;
        try {
            jaxbContext = JAXBContext.newInstance(typeClass);
            synchronized(this) {
                SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
                schema = schemaFactory.newSchema(this.getClass().getResource(schemaFile));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Class<? extends Entity> getEntityClass() {
        return clazz;
    }

    public Marshaller getMarshaller() throws IvoryException {
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            return marshaller;
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }
    
    public Unmarshaller getUnmarshaller() throws IvoryException {
        try{
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            unmarshaller.setSchema(schema);
            return unmarshaller;
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }
    
    public boolean isSchedulable() {
        return this.equals(EntityType.CLUSTER) ? false : true;
    }
}