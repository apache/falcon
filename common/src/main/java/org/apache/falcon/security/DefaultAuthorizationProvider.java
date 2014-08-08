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

    /**
     * The super-user is the user with the same identity as falcon process itself.
     * Loosely, if you started falcon, then you are the super-user.
     */
    protected static final String SUPER_USER = System.getProperty("user.name");

    /**
     * Constant for the configuration property that indicates the super user group.
     */
    private static final String SUPER_USER_GROUP_KEY = FALCON_PREFIX + "superusergroup";

    /**
     * Super ser group.
     */
    private String superUserGroup;
    private Set<String> adminUsers;
    private Set<String> adminGroups;

    public DefaultAuthorizationProvider() {
        superUserGroup = StartupProperties.get().getProperty(SUPER_USER_GROUP_KEY);
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
        Validate.isTrue(RESOURCES.contains(resource), "Illegal resource: " + resource);
        Validate.notEmpty(action, "Action cannot be empty or null");

        LOG.info("Authorizing authenticatedUser={}, against resource={}, action={}, entity name={}, "
                + "entity type={}", proxyUgi.getShortUserName(), resource, action, entityName, entityType);

        if (isSuperUser(proxyUgi)) {
            return;
        }

        if ("admin".equals(resource)) {
            if (!"version".equals(action)) {
                authorizeAdminResource(proxyUgi, action);
            }
        } else if ("entities".equals(resource) || "instance".equals(resource)) {
            authorizeEntityResource(proxyUgi, entityName, entityType, action);
        } else if ("lineage".equals(resource)) {
            authorizeLineageResource(proxyUgi.getShortUserName(), action);
        }
    }

    /**
     * Determines if the authenticated user is the user who started this process
     * or belongs to the super user group.
     *
     * @param authenticatedUGI UGI
     * @return true if super user else false.
     */
    protected boolean isSuperUser(UserGroupInformation authenticatedUGI) {
        final String authenticatedUser = authenticatedUGI.getShortUserName();
        return SUPER_USER.equals(authenticatedUser)
                || (!StringUtils.isEmpty(superUserGroup)
                    && isUserInGroup(superUserGroup, authenticatedUGI));
    }

    protected Set<String> getGroupNames(UserGroupInformation proxyUgi) {
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

        if (isSuperUser(proxyUgi)) {
            return;
        }
        
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
                || isUserInGroup(aclGroup, proxyUgi)) {
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

    /**
     * Determines if the authenticated user is the entity ACL owner.
     *
     * @param authenticatedUser authenticated user
     * @param aclOwner          entity ACL owner
     * @return true if authenticated user is the entity acl owner, false otherwise.
     */
    protected boolean isUserACLOwner(String authenticatedUser, String aclOwner) {
        return authenticatedUser.equals(aclOwner);
    }

    /**
     * Checks if the user's group matches the entity ACL group.
     *
     * @param group    Entity ACL group.
     * @param proxyUgi proxy ugi for the authenticated user.
     * @return true if user groups contains entity acl group.
     */
    protected boolean isUserInGroup(String group, UserGroupInformation proxyUgi) {
        Set<String> groups = getGroupNames(proxyUgi);
        return groups.contains(group);
    }

    /**
     * Check if the user has admin privileges.
     *
     * @param proxyUgi proxy ugi for the authenticated user.
     * @param action   admin action on the resource.
     * @throws AuthorizationException if the user does not have admin privileges.
     */
    protected void authorizeAdminResource(UserGroupInformation proxyUgi,
                                          String action) throws AuthorizationException {
        final String authenticatedUser = proxyUgi.getShortUserName();
        LOG.debug("Authorizing user={} for admin, action={}", authenticatedUser, action);
        if (adminUsers.contains(authenticatedUser) || isUserInAdminGroups(proxyUgi)) {
            return;
        }

        LOG.error("Permission denied: user {} does not have admin privilege for action={}",
                authenticatedUser, action);
        throw new AuthorizationException("Permission denied: user=" + authenticatedUser
                + " does not have admin privilege for action=" + action);
    }

    protected boolean isUserInAdminGroups(UserGroupInformation proxyUgi) {
        Set<String> groups = getGroupNames(proxyUgi);
        boolean isUserGroupInAdmin = false;
        for (String group : groups) {
            if (adminGroups.contains(group)) {
                isUserGroupInAdmin = true;
                break;
            }
        }

        return isUserGroupInAdmin;
    }

    protected void authorizeEntityResource(UserGroupInformation proxyUgi,
                                           String entityName, String entityType,
                                           String action) throws AuthorizationException {
        Validate.notEmpty(entityType, "Entity type cannot be empty or null");
        LOG.debug("Authorizing authenticatedUser={} against entity/instance action={}, "
                + "entity name={}, entity type={}",
                proxyUgi.getShortUserName(), action, entityName, entityType);

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
            break;
        }

        throw new AuthorizationException("Cannot get owner for entity: " + entity.getName());
    }

    protected void authorizeLineageResource(String authenticatedUser, String action) {
        LOG.debug("User {} authorized for action {} ", authenticatedUser, action);
        // todo - do nothing for now, read-only for all
    }
}
