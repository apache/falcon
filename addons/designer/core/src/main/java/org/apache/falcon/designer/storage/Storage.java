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

package org.apache.falcon.designer.storage;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * This is the storage on which entities can be persisted and restored from.
 *
 * On the storage entities are organized under a namespace. Namespace/entity combination
 * is unique on the storage.
 */
public interface Storage {

    /**
     * Opens an existing entity under the namespace and provides a Stream view of
     * that data for the consumer.
     *
     * @param namespace - Namespace under which the entity is stored.
     * @param entity - Entity that is being opened/read.
     * @return - InputStream
     * @throws StorageException - If such an entity doesn't exist or has issues
     * reading from the storage.
     */
    @Nonnull
    InputStream open(@Nonnull String namespace, @Nonnull String entity) throws StorageException;

    /**
     * Creates / Updates a new entity under the namespace and provides a Stream to write out the
     * data. If entity already exists under the namespace, same is overwritten.
     *
     * @param namespace - Namespace under which the entity is stored.
     * @param entity - Entity that is being created/updated.
     * @return - OutputStream
     * @throws StorageException - If it has issues accessing or writing to the storage.
     */
    @Nonnull
    OutputStream create(@Nonnull String namespace, @Nonnull String entity) throws StorageException;

    /**
     * Deletes an entity under the namespace specified if it exists.
     *
     * @param namespace - Namespace under which the entity is stored.
     * @param entity - Entity that is being deleted.
     * @throws StorageException - If entity is missing or if there are issues while performing the
     * delete operation
     */
    void delete(@Nonnull String namespace, @Nonnull String entity) throws StorageException;
}
