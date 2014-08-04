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

package org.apache.falcon.entity.v0;

import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;

import javax.xml.bind.*;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

/**
 * Enum for types of entities in Falcon Process, Feed and Cluster.
 */
public enum EntityType {
    FEED(Feed.class, "/feed-0.1.xsd", "name"),
    PROCESS(Process.class, "/process-0.1.xsd", "name"),
    CLUSTER(Cluster.class, "/cluster-0.1.xsd", "name");

    //Fail unmarshalling of whole xml if unmarshalling of any element fails
    private static class EventHandler implements ValidationEventHandler {
        @Override
        public boolean handleEvent(ValidationEvent event) {
            return false;
        }
    }

    private static final String NS = "http://www.w3.org/2001/XMLSchema";

    private final Class<? extends Entity> clazz;
    private JAXBContext jaxbContext;
    private Schema schema;
    private String[] immutableProperties;

    private String schemaFile;

    private EntityType(Class<? extends Entity> typeClass, String schemaFile, String... immutableProperties) {
        clazz = typeClass;
        this.immutableProperties = immutableProperties;
        this.schemaFile = schemaFile;
        try {
            jaxbContext = JAXBContext.newInstance(typeClass);
            synchronized (this) {
                SchemaFactory schemaFactory = SchemaFactory.newInstance(NS);
                schema = schemaFactory.newSchema(getClass().getResource(schemaFile));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Class<? extends Entity> getEntityClass() {
        return clazz;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public Marshaller getMarshaller() throws JAXBException {
        Marshaller marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        return marshaller;
    }

    public Unmarshaller getUnmarshaller() throws JAXBException {
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        //unmarshaller.setSchema(schema);
        unmarshaller.setEventHandler(new EventHandler());
        return unmarshaller;
    }

    public boolean isSchedulable() {
        return this != EntityType.CLUSTER;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP"})
    public String[] getImmutableProperties() {
        return immutableProperties;
    }
}
