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

package org.apache.ivory.entity.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigurationStore {

    private static final Logger LOG = Logger.getLogger(ConfigurationStore.class);
    private static final Logger AUDIT = Logger.getLogger("AUDIT");
    private static final String UTF_8 = "UTF-8";

    public static ConfigurationStore get() {
        return store;
    }

    private final Map<EntityType, ConcurrentHashMap<String, Entity>> dictionary =
            new HashMap<EntityType, ConcurrentHashMap<String, Entity>>();

    private final FileSystem fs;
    private final Marshaller marshaller;
    private final Unmarshaller unmarshaller;
    private final Path storePath;

    private static final Entity NULL = new Entity(){
        @Override
        public String getName() {
            return "NULL";
        }
    };

    private ConfigurationStore() {
        Class<? extends Entity>[] entityClasses =
                new Class[EntityType.values().length];

        int index = 0;

        for (EntityType type : EntityType.values()) {
            dictionary.put(type, new ConcurrentHashMap<String, Entity>());
            entityClasses[index++] = type.getEntityClass();
        }

        String uri = StartupProperties.get().getProperty("config.store.uri");
        storePath = new Path(uri);
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(entityClasses);
            marshaller = jaxbContext.createMarshaller();
            unmarshaller = jaxbContext.createUnmarshaller();
            fs = FileSystem.get(storePath.toUri(), new Configuration());

            bootstrap();
        } catch (Exception e) {
            throw new RuntimeException("Unable to bring up config store", e);
        }
    }

    private static final ConfigurationStore store = new ConfigurationStore();

    private void bootstrap() throws IOException {
        for (EntityType type : EntityType.values()) {
            ConcurrentHashMap<String, Entity> entityMap = dictionary.get(type);
            FileStatus[] files = fs.globStatus(new Path(storePath, type.name()+Path.SEPARATOR+"*"));
            if (files != null) {
                for (FileStatus file : files) {
                    String fileName = file.getPath().getName();
                    String encodedEntityName = fileName.substring(0,
                            fileName.length() - 4); //drop ".xml"
                    String entityName = URLDecoder.decode(encodedEntityName, UTF_8);
                    entityMap.put(entityName, NULL);
                }
            }
        }
    }

    /**
     *
     * @param type - EntityType that need to be published
     * @param entity - Reference to the Entity Object
     * @throws StoreAccessException If any error in accessing the storage
     */
    public synchronized void publish(EntityType type, Entity entity)
            throws StoreAccessException, EntityAlreadyExistsException {
        try {
            if (get(type, entity.getName()) == null) {
                persist(type, entity);
                dictionary.get(type).put(entity.getName(), entity);
            } else {
                throw new EntityAlreadyExistsException(type + ": " + entity.getName() +
                        " already registered with configuration store. " +
                        "Can't be submitted again. Try removing before submitting.");
            }
        } catch (IOException e) {
            throw new StoreAccessException(e);
        }
        AUDIT.info(type + "/" + entity.getName() +
                " is published into config store");
    }

    /**
     *
     * @param type - Entity type that is being retrieved
     * @param name - Name as it appears in the entity xml definition
     * @param <T> - Actual Entity object type
     * @return - Entity object from internal dictionary, If the object is not
     *            loaded in memory yet, it will retrieve it from persistent
     *            store just in time. On startup all the entities will be added
     *            to the dictionary with null reference.
     * @throws StoreAccessException If any error in accessing the storage
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> T get(EntityType type, String name)
            throws StoreAccessException {
        ConcurrentHashMap<String, Entity> entityMap = dictionary.get(type);
        if (entityMap.containsKey(name)) {
            T entity = (T) entityMap.get(name);
            if (entity == NULL) { //Object equality being checked
                try {
                    entity = restore(type, name);
                } catch (IOException e) {
                    throw new StoreAccessException(e);
                }
                entityMap.put(name, entity);
                return entity;
            } else {
                return entity;
            }
        } else {
            return null;
        }
    }

    public Collection<String> getEntities(EntityType type) {
        return Collections.
                unmodifiableCollection(dictionary.get(type).keySet());
    }

    /**
     * Remove an entity which is already stored in the config store
     * @param type - Entity type being removed
     * @param name - Name of the entity object being removed
     * @return - True is remove is successful, false if request entity doesn't
     *            exist
     * @throws StoreAccessException If any error when the config is archived
     */
    public boolean remove(EntityType type, String name)
            throws StoreAccessException {
        Map<String, Entity> entityMap = dictionary.get(type);
        if (entityMap.containsKey(name)) {
            try {
                archive(type, name);
                entityMap.remove(name);
            } catch (IOException e) {
                throw new StoreAccessException(e);
            }
            AUDIT.info(type + " " + name + " is removed from config store");
            return true;
        }
        return false;
    }

    /**
     *
     * @param type - Entity type that needs to be searched
     * @param keywords - List of keywords to search for. only entities that have
     *                    all the keywords being searched would be returned
     * @return - Array of entity types
     */
    public Entity[] search(EntityType type, String... keywords) {
        return null;//TODO
    }

    /**
     *
     * @param type - Entity type that is to be stored into persistent storage
     * @param entity - entity to persist. JAXB Annotated entity will be marshalled
     *                  to the persistent store. The convention used for storing
     *                  the object::
     *                  PROP(config.store.uri)/{entitytype}/{entityname}.xml
     * @throws StoreAccessException If any error in marshalling the object
     * @throws java.io.IOException  If any error in accessing the storage
     */
    private void persist(EntityType type, Entity entity)
            throws StoreAccessException, IOException {
        OutputStream out = fs.create(new Path(storePath, type +
                Path.SEPARATOR + URLEncoder.encode(entity.getName(), UTF_8) + ".xml"));
        try {
            marshaller.marshal(entity, out);
            LOG.info("Persisted configuration " + type + "/" + entity.getName());
        } catch (JAXBException e) {
            LOG.error(e);
            throw new StoreAccessException("Unable to serialize the entity object "
                    + type + "/" + entity.getName(), e);
        }finally{
            out.close();
        }
    }

    /**
     * Archive removed configuration in the persistent store
     * @param type - Entity type to archive
     * @param name - name
     * @throws IOException If any error in accessing the storage
     */
    private void archive(EntityType type, String name) throws IOException {
        Path archivePath = new Path(storePath, "archive" + Path.SEPARATOR + type);
        fs.mkdirs(archivePath);
        fs.rename(new Path(storePath, type + Path.SEPARATOR +
                URLEncoder.encode(name, UTF_8) + ".xml"), new Path(archivePath,
                URLEncoder.encode(name, UTF_8) + "." + System.currentTimeMillis()));
        LOG.info("Archived configuration " + type + "/" + name);
    }

    /**
     *
     * @param type - Entity type to restore from persistent store
     * @param name - Name of the entity to restore.
     * @param <T>  - Actual entity object type
     * @return     - De-serialized entity object restored from persistent store
     * @throws IOException If any error in accessing the storage
     * @throws StoreAccessException If any error unmarshalling
     */
    @SuppressWarnings("unchecked")
    private synchronized <T extends Entity> T restore(EntityType type,
                                                      String name)
            throws IOException, StoreAccessException {

        InputStream in = fs.open(new Path(storePath, type + Path.SEPARATOR +
                URLEncoder.encode(name, UTF_8) + ".xml"));
        try {
            return (T) unmarshaller.unmarshal(in);
        } catch(JAXBException e) {
            throw new StoreAccessException("Unable to un-marshall xml definition for "
                    + type + "/" + name, e);
        }  finally {
            in.close();
            LOG.info("Restored configuration " + type + "/" + name);
        }
    }
}
