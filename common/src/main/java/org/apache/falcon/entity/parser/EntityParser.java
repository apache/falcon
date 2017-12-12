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

package org.apache.falcon.entity.parser;

import org.apache.commons.io.IOUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Generic Abstract Entity Parser, the concrete FEED, PROCESS and CLUSTER should extend this parser
 * to implement specific parsing.
 *
 * @param <T> of type Entity
 */
public abstract class EntityParser<T extends Entity> {

    private static final Logger LOG = LoggerFactory.getLogger(EntityParser.class);

    private final EntityType entityType;
    protected final boolean isAuthorizationDisabled;

    protected EntityParser(EntityType entityType) {
        this.entityType = entityType;
        isAuthorizationDisabled = !SecurityUtil.isAuthorizationEnabled();
    }

    public EntityType getEntityType() {
        return this.entityType;
    }

    /**
     * Parses a sent XML and validates it using JAXB.
     *
     * @param xmlString - Entity XML
     * @return Entity - JAVA Object
     * @throws FalconException
     */
    public Entity parseAndValidate(String xmlString) throws FalconException {
        InputStream inputStream = null;
        try {
            inputStream = new ByteArrayInputStream(xmlString.getBytes());
            return parseAndValidate(inputStream);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    /**
     * Parses xml stream.
     *
     * @param xmlStream stream
     * @return entity
     * @throws FalconException
     */
    @SuppressWarnings("unchecked")
    public T parse(InputStream xmlStream) throws FalconException {
        try {
            XMLInputFactory xif = SchemaHelper.createXmlInputFactory();
            XMLStreamReader xsr = xif.createXMLStreamReader(xmlStream);
            // parse against schema
            Unmarshaller unmarshaller = entityType.getUnmarshaller();
            T entity = (T) unmarshaller.unmarshal(xsr);
            LOG.info("Parsed Entity: {}", entity.getName());
            return entity;
        } catch (Exception e) {
            throw new FalconException(e);
        }
    }

    public T parseAndValidate(InputStream xmlStream) throws FalconException {
        T entity = parse(xmlStream);
        validate(entity);
        return entity;
    }

    protected void validateEntityExists(EntityType type, String name) throws FalconException {
        if (ConfigurationStore.get().get(type, name) == null) {
            throw new ValidationException("Referenced " + type + " " + name + " is not registered");
        }
    }

    public abstract void validate(T entity) throws FalconException;

    /**
     * Checks if the acl owner is a valid user by fetching the groups for the owner.
     * Also checks if the acl group is one of the fetched groups for membership.
     * The only limitation is that a user cannot add a group in ACL that he does not belong to.
     *
     * @param acl  entity ACL
     * @throws org.apache.falcon.entity.parser.ValidationException
     */
    protected void validateACLOwnerAndGroup(AccessControlList acl) throws ValidationException {
        String aclOwner = acl.getOwner();
        String aclGroup = acl.getGroup();

        try {
            UserGroupInformation proxyACLUser = UserGroupInformation.createProxyUser(
                    aclOwner, UserGroupInformation.getLoginUser());
            Set<String> groups = new HashSet<String>(Arrays.asList(proxyACLUser.getGroupNames()));
            if (!groups.contains(aclGroup)) {
                throw new AuthorizationException("Invalid group: " + aclGroup
                        + " for user: " + aclOwner);
            }
        } catch (IOException e) {
            throw new ValidationException("Invalid acl owner " + aclOwner
                    + ", does not exist or does not belong to group: " + aclGroup);
        }
    }

    /**
     * Validate if the entity owner is the logged-in authenticated user.
     *
     * @param entityName  entity name
     * @param acl         entity ACL
     * @throws AuthorizationException
     */
    protected void authorize(String entityName,
                             AccessControlList acl) throws AuthorizationException {
        try {
            SecurityUtil.getAuthorizationProvider().authorizeEntity(entityName,
                    getEntityType().name(), acl, "submit", CurrentUser.getAuthenticatedUGI());
        } catch (FalconException e) {
            throw new AuthorizationException(e);
        } catch (IOException e) {
            throw new AuthorizationException(e);
        }
    }
}
