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

package org.apache.ivory.entity.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import javax.xml.bind.Unmarshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Pair;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.log4j.Logger;

/**
 * 
 * Generic Abstract Entity Parser, the concrete FEED, PROCESS and DATAENDPOINT
 * Should extend this parser to implement specific parsing.
 * 
 * @param <T>
 */
public abstract class EntityParser<T extends Entity> {

    private static final Logger LOG = Logger.getLogger(EntityParser.class);

    private final EntityType entityType;

    /**
     * Constructor
     * 
     * @param entityType
     *            - can be FEED or PROCESS
     * @param schemaFile
     */
    protected EntityParser(EntityType entityType) {
        this.entityType = entityType;
    }

    public EntityType getEntityType() {
        return this.entityType;
    }

    /**
     * Parses a sent XML and validates it using JAXB.
     * 
     * @param xmlString
     *            - Entity XML
     * @return Entity - JAVA Object
     * @throws IvoryException
     */
    public Entity parseAndValidate(String xmlString) throws IvoryException {
        InputStream inputStream = new ByteArrayInputStream(xmlString.getBytes());
        Entity entity = parseAndValidate(inputStream);
        return entity;
    }
    
    /**
     * Parses xml stream
     * 
     * @param xmlStream
     * @return entity
     * @throws IvoryException
     */
    public T parseAndValidate(InputStream xmlStream) throws IvoryException {
        try {
            // parse against schema
            Unmarshaller unmarshaller = entityType.getUnmarshaller();
            T entity = (T) unmarshaller.unmarshal(xmlStream);
            LOG.info("Parsed Entity: " + entity.getName());

            validate(entity);
            return entity;
        } catch (ValidationException e) {
            throw e;
        } catch(Exception e) {
            throw new IvoryException(e);
        }
    }

    protected void validateEntityExists(EntityType type, String name) throws IvoryException {
        if(ConfigurationStore.get().get(type, name) == null)
            throw new ValidationException("Referenced " + type + " " + name + " is not registered");        
    }
    
    protected void validateEntitiesExist(List<Pair<EntityType, String>> entities) throws IvoryException {
        if(entities != null) {
            for(Pair<EntityType, String> entity:entities) {
                validateEntityExists(entity.first, entity.second);
            }
        }
    }
    
    public abstract void validate(T entity) throws IvoryException;
}