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

package org.apache.falcon.designer.schema;

import org.apache.falcon.designer.storage.Storage;
import org.apache.falcon.designer.storage.StorageException;
import org.apache.falcon.designer.storage.Storeable;

import javax.annotation.Nonnull;
import java.sql.Types;

/**
 * Relational Schema allows data to be represented similar to a relational
 * table comprising of rows and columns. Data types for each column is
 * allowed to be both primitive and complex data types ranging from simple
 * integer to complex structures such as {@link java.util.Map},
 * {@link java.util.Set}, {@link java.util.List}, Thrift structure and
 * Protobuf messages etc.
 */
public class RelationalSchema implements Storeable {

    /**
     * Gets the total number of columns present in the
     * underlying data source that conforms to this schema.
     *
     * @return Total Number of columns.
     */
    public int getColumnCount() {
        return -1;
    }

    /**
     * Gets the designated alias of a given column number
     * as visible in the underlying data source.
     *
     * @param columnNumber - Can range from 0 to totalColumns - 1
     * @return Alias of the column
     */
    public String getColumnAlias(int columnNumber) {
        return "";
    }

    /**
     * Gets the column type of a given column as visible
     * in the underlying data source.
     *
     * @param columnNumber - Can range from 0 to totalColumns - 1
     * @return {@link java.sql.Types}
     */
    public int getColumnType(int columnNumber) {
        return Types.NULL;
    }

    @Override
    public void store(@Nonnull Storage storage) throws StorageException {
        //TODO
    }

    @Override
    public void restore(@Nonnull Storage storage) throws StorageException {
        //TODO
    }

    @Override
    public void delete(@Nonnull Storage storage) throws StorageException {
        //TODO
    }
}
