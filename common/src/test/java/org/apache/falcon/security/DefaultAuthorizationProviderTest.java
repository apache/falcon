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

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EntityBuilderTestUtil;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collection;

/**
 * Unit tests for DefaultAuthorizationProvider.
 */
public class DefaultAuthorizationProviderTest {

    public static final String CLUSTER_ENTITY_NAME = "primary-cluster";
    public static final String PROCESS_ENTITY_NAME = "sample-process";

    private UserGroupInformation realUser;
    private ConfigurationStore configStore;
    private Cluster clusterEntity;
    private Feed feedEntity;
    private org.apache.falcon.entity.v0.process.Process processEntity;

    @BeforeClass
    public void setUp() throws Exception {
        realUser = UserGroupInformation.createUserForTesting("falcon", new String[]{"falcon", });

        CurrentUser.authenticate(EntityBuilderTestUtil.USER);
        org.testng.Assert.assertEquals(CurrentUser.getUser(), EntityBuilderTestUtil.USER);

        configStore = ConfigurationStore.get();

        addClusterEntity();
        addFeedEntity();
        addProcessEntity();
        org.testng.Assert.assertNotNull(processEntity);
    }

    public void addClusterEntity() throws Exception {
        clusterEntity = EntityBuilderTestUtil.buildCluster(CLUSTER_ENTITY_NAME);
        configStore.publish(EntityType.CLUSTER, clusterEntity);
    }

    public void addFeedEntity() throws Exception {
        feedEntity = EntityBuilderTestUtil.buildFeed("sample-feed", clusterEntity,
                "classified-as=Secure", "analytics");
        addStorage(feedEntity, Storage.TYPE.FILESYSTEM, "/falcon/impression-feed/${YEAR}/${MONTH}/${DAY}");
        configStore.publish(EntityType.FEED, feedEntity);
    }

    private static void addStorage(Feed feed, Storage.TYPE storageType, String uriTemplate) {
        if (storageType == Storage.TYPE.FILESYSTEM) {
            Locations locations = new Locations();
            feed.setLocations(locations);

            Location location = new Location();
            location.setType(LocationType.DATA);
            location.setPath(uriTemplate);
            feed.getLocations().getLocations().add(location);
        } else {
            CatalogTable table = new CatalogTable();
            table.setUri(uriTemplate);
            feed.setTable(table);
        }
    }

    public void addProcessEntity() throws Exception {
        processEntity = EntityBuilderTestUtil.buildProcess(PROCESS_ENTITY_NAME,
                clusterEntity, "classified-as=Critical");
        EntityBuilderTestUtil.addProcessWorkflow(processEntity);
        EntityBuilderTestUtil.addProcessACL(processEntity);

        configStore.publish(EntityType.PROCESS, processEntity);
    }

    @AfterClass
    public void tearDown() throws Exception {
        cleanupStore();
    }

    protected void cleanupStore() throws FalconException {
        configStore = ConfigurationStore.get();
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = configStore.getEntities(type);
            for (String entity : entities) {
                configStore.remove(type, entity);
            }
        }
    }

    @Test
    public void testAuthorizeAdminResourceVersionAction() throws Exception {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "blah", realUser, new String[]{"blah-group", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("admin", "version", null, null, proxyUgi);
    }

    @Test
    public void testAuthorizeSuperUser() throws Exception {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                EntityBuilderTestUtil.USER, realUser, new String[]{"group", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("entities", "schedule", "feed", feedEntity.getName(), proxyUgi);
        provider.authorizeResource("instance", "status", "feed", feedEntity.getName(), proxyUgi);
    }

    @Test
    public void testAuthorizeSuperUserGroup() throws Exception {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "blah", realUser, new String[]{"falcon", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("entities", "schedule", "feed", feedEntity.getName(), proxyUgi);
        provider.authorizeResource("instance", "status", "feed", feedEntity.getName(), proxyUgi);
    }

    @DataProvider(name = "adminResourceActions")
    private Object[][] createAdminResourceActions() {
        return new Object[][] {
            {"version"},
            {"stack"},
            {"config"},
        };
    }

    @Test (dataProvider = "adminResourceActions")
    public void testAuthorizeAdminResourceAdmin(String action) throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("admin", action, null, null, proxyUgi);
    }

    @Test
    public void testAuthorizeAdminResourceAdminUserBadGroup() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin-group", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("admin", "version", null, null, proxyUgi);
    }

    @Test
    public void testAuthorizeAdminResourceAdminGroupBadUser() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty(
                "falcon.security.authorization.admin.groups", "admin-group");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin-user", realUser, new String[]{"admin-group", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("admin", "version", null, null, proxyUgi);
    }

    @Test (expectedExceptions = AuthorizationException.class)
    public void testAuthorizeAdminResourceInvalidUserAndGroup() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin-user", realUser, new String[]{"admin-group", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("admin", "stack", null, null, proxyUgi);
        Assert.fail("User does not belong to both admin-users not groups");
    }

    @DataProvider(name = "entityResourceActions")
    private Object[][] createEntityResourceActions() {
        return new Object[][] {
            {"entities", "list", "feed"},
            {"entities", "list", "process"},
            {"entities", "list", "cluster"},
        };
    }

    @Test (dataProvider = "entityResourceActions")
    public void testAuthorizeEntitiesInstancesReadOnlyResource(String resource,
                                                               String action,
                                                               String entityType) throws Exception {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin-user", realUser, new String[]{"admin-group", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource(resource, action, entityType, null, proxyUgi);
    }

    @DataProvider(name = "entityLifecycleResourceActions")
    private Object[][] createEntityLifecycleResourceActions() {
        return new Object[][] {
            {"entities", "status", "cluster", "primary-cluster"},
            {"entities", "status", "process", "sample-process"},
            {"entities", "status", "feed", "sample-feed"},
            {"instance", "status", "process", "sample-process"},
            {"instance", "running", "process", "sample-process"},
            {"instance", "running", "feed", "sample-feed"},
        };
    }

    @Test(dataProvider = "entityLifecycleResourceActions")
    public void testAuthorizeEntitiesInstancesLifecycleResource(String resource, String action,
                                                                String entityType,
                                                                String entityName) throws Exception {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                EntityBuilderTestUtil.USER, realUser, new String[]{EntityBuilderTestUtil.USER, });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource(resource, action, entityType, entityName, proxyUgi);
    }

    @Test(dataProvider = "entityLifecycleResourceActions",
            expectedExceptions = AuthorizationException.class)
    public void testAuthorizeEntitiesInstancesLifecycleResourceBadUGI(String resource,
                                                                      String action,
                                                                      String entityType,
                                                                      String entityName)
        throws Exception {
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin-user", realUser, new String[]{"admin-group", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource(resource, action, entityType, entityName, proxyUgi);
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testAuthorizeBadResource() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("invalid", "version", null, null, proxyUgi);
        Assert.fail("Bad resource");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testAuthorizeNullResource() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource(null, "version", null, null, proxyUgi);
        Assert.fail("Bad resource");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testAuthorizeBadAction() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("entities", null, "feedz", null, proxyUgi);
        Assert.fail("Bad action");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testAuthorizeNullEntityType() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("entities", "list", null, "primary-cluster", proxyUgi);
        Assert.fail("Bad entity type");
    }

    @Test (expectedExceptions = IllegalArgumentException.class)
    public void testAuthorizeBadEntityType() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("entities", "list", "clusterz", "primary-cluster", proxyUgi);
        Assert.fail("Bad entity type");
    }

    @Test
    public void testAuthorizeValidatePOSTOperations() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        EntityBuilderTestUtil.addProcessACL(processEntity, "admin", "admin");
        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeEntity(processEntity.getName(), "process",
                processEntity.getACL(), "submit", proxyUgi);
    }

    @Test (expectedExceptions = EntityNotRegisteredException.class)
    public void testAuthorizeResourceOperationsBadEntity() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("entities", "status", "process", feedEntity.getName(), proxyUgi);
        Assert.fail("Bad entity");
    }

    @Test
    public void testAuthorizeValidatePOSTOperationsGroupBadUser() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        EntityBuilderTestUtil.addProcessACL(processEntity, "admin-user", "admin");
        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeEntity(processEntity.getName(), "process",
                processEntity.getACL(), "submit", proxyUgi);
    }

    @Test (expectedExceptions = AuthorizationException.class)
    public void testAuthorizeValidatePOSTOperationsBadUserAndGroup() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        EntityBuilderTestUtil.addProcessACL(processEntity, "admin-user", "admin-group");
        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeEntity(processEntity.getName(), "process",
                processEntity.getACL(), "submit", proxyUgi);
    }

    @Test
    public void testAuthorizeLineageResource() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.admin.users", "admin");
        StartupProperties.get().setProperty("falcon.security.authorization.admin.groups", "admin");

        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUserForTesting(
                "admin", realUser, new String[]{"admin", });

        DefaultAuthorizationProvider provider = new DefaultAuthorizationProvider();
        provider.authorizeResource("metadata", "lineage", null, null, proxyUgi);
    }
}
