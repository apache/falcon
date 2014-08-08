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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Default implementation of AuthorizationProvider in Falcon.
 *
 * The authorization is enforced in the following way:
 *
 * if admin resource,
 *      if authenticated user name matches the admin users configuration
 *      Else if groups of the authenticated user matches the admin groups configuration
 * Else if entities or instance resource
 *      if the authenticated user matches the owner in ACL for the entity
 *      Else if the groups of the authenticated user matches the group in ACL for the entity
 * Else if lineage resource
 *      All have read-only permissions
 * Else bad resource
 */
public class DefaultAuthorizationProvider implements AuthorizationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthorizationProvider.class);

    private static final Set<String> RESOURCES = new HashSet<String>(
            Arrays.asList(new String[]{"admin", "entities", "instance", "lineage", }));

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    protected static final String FALCON_PREFIX = "falcon.security.authorization.";

    /**
     * Constant for the configuration property that indicates the blacklisted super users for falcon.
     */
    private static final String ADMIN_USERS_KEY = FALCON_PREFIX + "admin.users";
    private static final String ADMIN_GROUPS_KEY = FALCON_PREFIX + "admin.groups";

    private Set<String> adminUsers;
    private Set<String> adminGroups;

    public DefaultAuthorizationProvider() {
        adminUsers = getAdminNamesFromConfig(ADMIN_USERS_KEY);
        adminGroups = getAdminNamesFromConfig(ADMIN_GROUPS_KEY);
    }

    private HashSet<String> getAdminNamesFromConfig(String key) {
        HashSet<String> adminNames = new HashSet<String>();
        String adminNamesConfig = StartupProperties.get().getProperty(key);
        if (!StringUtils.isEmpty(adminNamesConfig)) {
            adminNames.addAll(Arrays.asList(adminNamesConfig.split(",")));
        }

        return adminNames;
    }

    /**
     * Determines if the authenticated user is authorized to execute the action on the resource.
     * Throws an exception if not authorized.
     *
     * @param resource   api resource, admin, entities or instance
     * @param action     action being authorized on resource and entity if applicable
     * @param entityType entity type in question, not for admin resource
     * @param entityName entity name in question, not for admin resource
     * @param proxyUgi   proxy ugi for the authenticated user
     * @throws org.apache.hadoop.security.authorize.AuthorizationException
     */
    @Override
    public void authorizeResource(String resource, String action,
                                  String entityType, String entityName,
                                  UserGroupInformation proxyUgi) throws AuthorizationException {
        Validate.notEmpty(resource, "Resource cannot be empty or null");
        Validate.notEmpty(action, "Action cannot be empty or null");

        Set<String> groups = getGroupNames(proxyUgi);
        String authenticatedUser = proxyUgi.getShortUserName();
        LOG.info("Authorizing authenticatedUser={}, groups={} against resource={}, action={}, entity name={}, "
                + "entity type={}", authenticatedUser, groups, resource, action, entityName, entityType);

        if ("admin".equals(resource)) {
            authorizeAdminResource(authenticatedUser, groups, action);
        } else if ("entities".equals(resource) || "instance".equals(resource)) {
            authorizeEntityResource(authenticatedUser, proxyUgi, entityName, entityType, action);
        } else if ("lineage".equals(resource)) {
            authorizeLineageResource(authenticatedUser, action);
        } else {
            throw new AuthorizationException("Unknown resource: " + resource);
        }
    }

    private Set<String> getGroupNames(UserGroupInformation proxyUgi) {
        HashSet<String> s = new HashSet<String>(Arrays.asList(proxyUgi.getGroupNames()));
        return Collections.unmodifiableSet(s);
    }

    /**
     * Determines if the authenticated user is authorized to execute the action on the entity.
     * Throws an exception if not authorized.
     *
     * @param entityName entity in question, applicable for entities and instance resource
     * @param entityType entity in question, applicable for entities and instance resource
     * @param acl        entity ACL
     * @param action     action being authorized on resource and entity if applicable
     * @param proxyUgi   proxy ugi for the authenticated user
     * @throws org.apache.hadoop.security.authorize.AuthorizationException
     */
    @Override
    public void authorizeEntity(String entityName, String entityType,
                                AccessControlList acl, String action,
                                UserGroupInformation proxyUgi) throws AuthorizationException {
        String authenticatedUser = proxyUgi.getShortUserName();
        LOG.info("Authorizing authenticatedUser={}, action={}, entity={}, type{}",
                authenticatedUser, action, entityName, entityType);

        checkUser(entityName, acl.getOwner(), acl.getGroup(), action, authenticatedUser, proxyUgi);
    }

    /**
     * Validate if the entity owner is the logged-in authenticated user.
     *
     * @param entityName        entity name.
     * @param aclOwner          entity ACL Owner.
     * @param aclGroup          entity ACL group.
     * @param action            action being authorized on resource and entity if applicable.
     * @param authenticatedUser authenticated user name.
     * @param proxyUgi          proxy ugi for the authenticated user.
     * @throws AuthorizationException
     */
    protected void checkUser(String entityName, String aclOwner, String aclGroup,
                             String action, String authenticatedUser,
                             UserGroupInformation proxyUgi) throws AuthorizationException {
        if (isUserACLOwner(authenticatedUser, aclOwner)
                || isUserInAclGroup(aclGroup, proxyUgi)) {
            return;
        }

        StringBuilder message = new StringBuilder("Permission denied: authenticatedUser=");
        message.append(authenticatedUser);
        message.append(!authenticatedUser.equals(aclOwner)
                ? " not entity owner=" + aclOwner
                : " not in group=" + aclGroup);
        message.append(", entity=").append(entityName).append(", action=").append(action);

        LOG.error(message.toString());
        throw new AuthorizationException(message.toString());
    }

    protected boolean isUserACLOwner(String authenticatedUser, String aclOwner) {
        return authenticatedUser.equals(aclOwner);
    }

    protected boolean isUserInAclGroup(String aclGroup, UserGroupInformation proxyUgi) {
        Set<String> groups = getGroupNames(proxyUgi);
        return groups.contains(aclGroup);
    }

    /**
     * Check if the user has admin privileges.
     *
     * @param user   user name.
     * @param groups groups that the user belongs to.
     * @param action admin action on the resource
     * @throws AuthorizationException if the user does not have admin privileges.
     */
    protected void authorizeAdminResource(String user, Set<String> groups,
                                          String action) throws AuthorizationException {
        LOG.debug("Authorizing user={} for admin, action={}", user, action);
        if (adminUsers.contains(user) || isUserInAdminGroups(groups)) {
            return;
        }

        LOG.error("Permission denied: user {} does not have admin privilege for action={}",
                user, action);
        throw new AuthorizationException("Permission denied: user=" + user
                + " does not have admin privilege for action=" + action);
    }

    protected boolean isUserInAdminGroups(Set<String> groups) {
        boolean isUserGroupInAdmin = false;
        for (String group : groups) {
            if (adminGroups.contains(group)) {
                isUserGroupInAdmin = true;
                break;
            }
        }

        return isUserGroupInAdmin;
    }

    protected void authorizeEntityResource(String authenticatedUser, UserGroupInformation proxyUgi,
                                           String entityName, String entityType,
                                           String action) throws AuthorizationException {
        Validate.notEmpty(entityType, "Entity type cannot be empty or null");
        LOG.debug("Authorizing authenticatedUser={} against entity/instance action={}, "
                + "entity name={}, entity type={}", authenticatedUser, action, entityName,
                entityType);

        if (entityName != null) { // lifecycle actions
            Entity entity = getEntity(entityName, entityType);
            authorizeEntity(entity.getName(), entity.getEntityType().name(),
                    getACL(entity), action, proxyUgi);
        } else {
            // non lifecycle actions, lifecycle actions with null entity will validate later
            LOG.info("Authorization for action={} will be done in the API", action);
        }
    }

    private Entity getEntity(String entityName, String entityType) throws AuthorizationException {
        try {
            EntityType type = EntityType.valueOf(entityType.toUpperCase());
            return EntityUtil.getEntity(type, entityName);
        } catch (FalconException e) {
            throw new AuthorizationException(e);
        }
    }

    protected AccessControlList getACL(Entity entity) throws AuthorizationException {
        switch (entity.getEntityType()) {
        case CLUSTER:
            return ((Cluster) entity).getACL();

        case FEED:
            return ((org.apache.falcon.entity.v0.feed.Feed) entity).getACL();

        case PROCESS:
            return ((org.apache.falcon.entity.v0.process.Process) entity).getACL();

        default:
            throw new AuthorizationException("Cannot get owner for entity: " + entity.getName());
        }
    }

    protected void authorizeLineageResource(String authenticatedUser, String action) {
        LOG.debug("User {} authorized for action {} ", authenticatedUser, action);
        // todo - do nothing for now, read-only for all
    }
}
