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

package org.apache.falcon.security;

import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;

/**
 * An interface for authorizing user against an entity operation.
 */
public interface AuthorizationProvider {

    /**
     * Check if the authenticated user is a super user.
     *
     * @param proxyUgi   proxy ugi for the authenticated user
     * @return true if sure user, else false
     */
    boolean isSuperUser(UserGroupInformation proxyUgi);

    /**
     * Determines if the authenticated user is authorized to execute the action on the resource,
     * which is typically a REST resource path.
     * Throws an exception if not authorized.
     *
     * @param resource   api resource, admin, entities or instance
     * @param action     action being authorized on resource and entity if applicable
     * @param entityType entity type in question, not for admin resource
     * @param entityName entity name in question, not for admin resource
     * @param proxyUgi   proxy ugi for the authenticated user
     * @throws AuthorizationException
     */
    void authorizeResource(String resource,
                           String action,
                           String entityType,
                           String entityName,
                           UserGroupInformation proxyUgi) throws AuthorizationException;

    /**
     * Determines if the authenticated user is authorized to execute the action on the entity.
     * Throws an exception if not authorized.
     *
     * @param entityName entity in question, applicable for entities and instance resource
     * @param entityType entity in question, applicable for entities and instance resource
     * @param acl        entity ACL
     * @param action     action being authorized on resource and entity if applicable
     * @param proxyUgi   proxy ugi for the authenticated user
     * @throws AuthorizationException
     */
    void authorizeEntity(String entityName, String entityType,
                         AccessControlList acl, String action,
                         UserGroupInformation proxyUgi) throws AuthorizationException;
}
