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

import java.io.InputStream;
import java.util.List;

import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.EntityAlreadyExistsException;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.log4j.Logger;

import com.sun.tools.javac.util.Pair;

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

    private String schemaFile;

    /**
     * Constructor
     * 
     * @param entityType
     *            - can be FEED or PROCESS
     * @param schemaFile
     */
    protected EntityParser(EntityType entityType, String schemaFile) {
        this.entityType = entityType;
        this.schemaFile = schemaFile;
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
    public Entity parse(String xmlString) throws IvoryException {
        InputStream inputStream = Util.getStreamFromString(xmlString);
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
    public T parseAndValidate(InputStream xmlStream) throws ValidationException {
        try {
            // parse against schema
            Unmarshaller unmarshaller = Util.getUnmarshaller(entityType.getEntityClass());
            Schema schema = Util.getSchema(ClusterEntityParser.class.getResource(schemaFile));
            unmarshaller.setSchema(schema);
            T entity = (T) unmarshaller.unmarshal(xmlStream);
            LOG.info("Parsed Entity: " + entity.getName());

            validate(entity);
            return entity;
        } catch (Exception e) {
            throw new ValidationException(e);
        }
    }

    protected void validateEntitiesExist(List<Pair<EntityType, String>> entities) throws ValidationException, StoreAccessException {
        if(entities != null) {
            ConfigurationStore store = ConfigurationStore.get();
            for(Pair<EntityType, String> entity:entities) {
                if(store.get(entity.fst, entity.snd) == null)
                    throw new ValidationException("Referenced " + entity.fst + " " + entity.snd + " is not registered");
            }
        }
    }
    
    protected abstract void validate(T entity) throws ValidationException, StoreAccessException;
}