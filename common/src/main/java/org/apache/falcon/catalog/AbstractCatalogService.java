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

package org.apache.falcon.catalog;

import org.apache.falcon.FalconException;

import java.util.Map;

/**
 * Interface definition for a catalog registry service
 * such as Hive or HCatalog.
 */
public abstract class AbstractCatalogService {

    /**
     * This method checks if the catalog service is alive.
     *
     * @param catalogBaseUrl url for the catalog service
     * @return if the service was reachable
     * @throws FalconException exception
     */
    public abstract boolean isAlive(String catalogBaseUrl) throws FalconException;

    /**
     * This method checks if the given table exists in the catalog.
     *
     * @param catalogUrl url for the catalog service
     * @param database database the table belongs to
     * @param tableName tableName to check if it exists
     * @return if the table exists
     * @throws FalconException exception
     */
    public abstract boolean tableExists(String catalogUrl, String database, String tableName)
        throws FalconException;

    /**
     * Returns a list of table properties. Most important here are:
     * 1. Table type: external table or a managed table
     * 2. Location on HDFS
     *
     * @param catalogUrl url for the catalog service
     * @param database database the table belongs to
     * @param tableName tableName to check if it exists
     * @return Bag of property name and associated value
     * @throws FalconException
     */
    public abstract Map<String, String> listTableProperties(String catalogUrl, String database,
                                                            String tableName) throws FalconException;
}
