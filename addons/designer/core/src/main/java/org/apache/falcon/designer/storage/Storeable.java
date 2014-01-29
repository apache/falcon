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

/**
 * All elements in the designer that need to be persisted to permanent
 * storage need to implement this interface.
 */
public interface Storeable {

    /**
     * Store the current object onto the storage being passed.
     *
     * @param storage - Storage onto which the object will be persisted or stored.
     * @throws StorageException - Any exception from the underlying storage.
     */
    void store(@Nonnull Storage storage) throws StorageException;

    /**
     * Restore onto the current object contents from the Storage.
     *
     * @param storage - Storage from where the object will be restored from.
     * @throws StorageException - Any exception from the underlying storage.
     */
    void restore(@Nonnull Storage storage) throws StorageException;

    /**
     * Deletes the current object from the storage permanently.
     *
     * @param storage - Storage on which the object is stored, that needs to be deleted
     * @throws StorageException - Any exception from the underlying storage.
     */
    void delete(@Nonnull Storage storage) throws StorageException;
}
