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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
            Arrays.asList(new String[]{"admin", "entities", "instance", "metadata", }));

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
     * Super user group.
     */
    private final String superUserGroup;
    private final Set<String> adminUsers;
    private final Set<String> adminGroups;

    public DefaultAuthorizationProvider() {
        superUserGroup = StartupProperties.get().getProperty(SUPER_USER_GROUP_KEY);
        adminUsers = getAdminNamesFromConfig(ADMIN_USERS_KEY);
        adminGroups = getAdminNamesFromConfig(ADMIN_GROUPS_KEY);
    }

    private Set<String> getAdminNamesFromConfig(String key) {
        Set<String> adminNames = new HashSet<String>();
        String adminNamesConfig = StartupProperties.get().getProperty(key);
        if (!StringUtils.isEmpty(adminNamesConfig)) {
            adminNames.addAll(Arrays.asList(adminNamesConfig.split(",")));
        }

        return Collections.unmodifiableSet(adminNames);
    }

    /**
     * Determines if the authenticated user is the user who started this process
     * or belongs to the super user group.
     *
     * @param authenticatedUGI UGI
     * @return true if super user else false.
     */
    public boolean isSuperUser(UserGroupInformation authenticatedUGI) {
        return SUPER_USER.equals(authenticatedUGI.getShortUserName())
            || (!StringUtils.isEmpty(superUserGroup)
                    && isUserInGroup(superUserGroup, authenticatedUGI));
    }

    /**
     * Checks if authenticated user should proxy the entity acl owner.
     *
     * @param authenticatedUGI  proxy ugi for the authenticated user.
     * @param aclOwner          entity ACL Owner.
     * @param aclGroup          entity ACL group.
     * @throws IOException
     */
    @Override
    public boolean shouldProxy(UserGroupInformation authenticatedUGI,
                               final String aclOwner,
                               final String aclGroup) throws IOException {
        Validate.notNull(authenticatedUGI, "User cannot be empty or null");
        Validate.notEmpty(aclOwner, "User cannot be empty or null");
        Validate.notEmpty(aclGroup, "Group cannot be empty or null");

        return isSuperUser(authenticatedUGI)
            || (!isUserACLOwner(authenticatedUGI.getShortUserName(), aclOwner)
                    && isUserInGroup(aclGroup, authenticatedUGI));
    }

    /**
     * Determines if the authenticated user is authorized to execute the action on the resource.
     * Throws an exception if not authorized.
     *
     * @param resource   api resource, admin, entities or instance
     * @param action     action being authorized on resource and entity if applicable
     * @param entityType entity type in question, not for admin resource
     * @param entityName entity name in question, not for admin resource
     * @param authenticatedUGI   proxy ugi for the authenticated user
     * @throws org.apache.hadoop.security.authorize.AuthorizationException
     */
    @Override
    public void authorizeResource(String resource, String action,
                                  String entityType, String entityName,
                                  UserGroupInformation authenticatedUGI)
        throws AuthorizationException, EntityNotRegisteredException {

        Validate.notEmpty(resource, "Resource cannot be empty or null");
        Validate.isTrue(RESOURCES.contains(resource), "Illegal resource: " + resource);
        Validate.notEmpty(action, "Action cannot be empty or null");

        try {
            if (isSuperUser(authenticatedUGI)) {
                return;
            }

            if ("admin".equals(resource)) {
                if (!"version".equals(action)) {
                    authorizeAdminResource(authenticatedUGI, action);
                }
            } else if ("entities".equals(resource) || "instance".equals(resource)) {
                authorizeEntityResource(authenticatedUGI, entityName, entityType, action);
            } else if ("metadata".equals(resource)) {
                authorizeMetadataResource(authenticatedUGI, action);
            }
        } catch (IOException e) {
            throw new AuthorizationException(e);
        }
    }

    protected Set<String> getGroupNames(UserGroupInformation proxyUgi) {
        return new HashSet<String>(Arrays.asList(proxyUgi.getGroupNames()));
    }

    /**
     * Determines if the authenticated user is authorized to execute the action on the entity.
     * Throws an exception if not authorized.
     *
     * @param entityName entity in question, applicable for entities and instance resource
     * @param entityType entity in question, applicable for entities and instance resource
     * @param acl        entity ACL
     * @param action     action being authorized on resource and entity if applicable
     * @param authenticatedUGI   proxy ugi for the authenticated user
     * @throws org.apache.hadoop.security.authorize.AuthorizationException
     */
    @Override
    public void authorizeEntity(String entityName, String entityType, AccessControlList acl,
                                String action, UserGroupInformation authenticatedUGI)
        throws AuthorizationException {

        try {
            LOG.info("Authorizing authenticatedUser={}, action={}, entity={}, type{}",
                    authenticatedUGI.getShortUserName(), action, entityName, entityType);

            if (isSuperUser(authenticatedUGI)) {
                return;
            }

            checkUser(entityName, acl.getOwner(), acl.getGroup(), action, authenticatedUGI);
        } catch (IOException e) {
            throw new AuthorizationException(e);
        }
    }

    /**
     * Validate if the entity owner is the logged-in authenticated user.
     *
     * @param entityName        entity name.
     * @param aclOwner          entity ACL Owner.
     * @param aclGroup          entity ACL group.
     * @param action            action being authorized on resource and entity if applicable.
     * @param authenticatedUGI          proxy ugi for the authenticated user.
     * @throws AuthorizationException
     */
    protected void checkUser(String entityName, String aclOwner, String aclGroup, String action,
                             UserGroupInformation authenticatedUGI) throws AuthorizationException {
        final String authenticatedUser = authenticatedUGI.getShortUserName();
        if (isUserACLOwner(authenticatedUser, aclOwner)
                || isUserInGroup(aclGroup, authenticatedUGI)) {
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
     * @param authenticatedUGI proxy ugi for the authenticated user.
     * @param action   admin action on the resource.
     * @throws AuthorizationException if the user does not have admin privileges.
     */
    protected void authorizeAdminResource(UserGroupInformation authenticatedUGI,
                                          String action) throws AuthorizationException {
        final String authenticatedUser = authenticatedUGI.getShortUserName();
        LOG.debug("Authorizing user={} for admin, action={}", authenticatedUser, action);
        if (adminUsers.contains(authenticatedUser) || isUserInAdminGroups(authenticatedUGI)) {
            return;
        }

        LOG.error("Permission denied: user {} does not have admin privilege for action={}",
                authenticatedUser, action);
        throw new AuthorizationException("Permission denied: user=" + authenticatedUser
                + " does not have admin privilege for action=" + action);
    }

    protected boolean isUserInAdminGroups(UserGroupInformation proxyUgi) {
        final Set<String> groups = getGroupNames(proxyUgi);
        groups.retainAll(adminGroups);
        return !groups.isEmpty();
    }

    protected void authorizeEntityResource(UserGroupInformation authenticatedUGI,
                                           String entityName, String entityType,
                                           String action)
        throws AuthorizationException, EntityNotRegisteredException {

        Validate.notEmpty(entityType, "Entity type cannot be empty or null");
        LOG.debug("Authorizing authenticatedUser={} against entity/instance action={}, "
                + "entity name={}, entity type={}",
                authenticatedUGI.getShortUserName(), action, entityName, entityType);

        if (entityName != null) { // lifecycle actions
            Entity entity = getEntity(entityName, entityType);
            authorizeEntity(entity.getName(), entity.getEntityType().name(),
                entity.getACL(), action, authenticatedUGI);
        } else {
            // non lifecycle actions, lifecycle actions with null entity will validate later
            LOG.info("Authorization for action={} will be done in the API", action);
        }
    }

    private Entity getEntity(String entityName, String entityType)
        throws EntityNotRegisteredException, AuthorizationException {

        try {
            EntityType type = EntityType.getEnum(entityType);
            return EntityUtil.getEntity(type, entityName);
        } catch (FalconException e) {
            if (e instanceof EntityNotRegisteredException) {
                throw (EntityNotRegisteredException) e;
            } else {
                throw new AuthorizationException(e);
            }
        }
    }

    protected void authorizeMetadataResource(UserGroupInformation authenticatedUGI,
                                             String action) throws AuthorizationException {
        LOG.debug("User {} authorized for action {} ", authenticatedUGI.getShortUserName(), action);
        // todo - read-only for all metadata but needs to be implemented
    }
}
