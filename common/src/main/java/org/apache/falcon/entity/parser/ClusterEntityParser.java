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

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.ACL;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Properties;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.util.OozieConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;

/**
 * Parser that parses cluster entity definition.
 */
public class ClusterEntityParser extends EntityParser<Cluster> {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterEntityParser.class);

    public ClusterEntityParser() {
        super(EntityType.CLUSTER);
    }

    @Override
    public void validate(Cluster cluster) throws ValidationException {
        // validating scheme in light of fail-early
        validateScheme(cluster, Interfacetype.READONLY);
        validateScheme(cluster, Interfacetype.WRITE);
        validateScheme(cluster, Interfacetype.WORKFLOW);
        // User may choose to disable job completion notifications
        if (ClusterHelper.getInterface(cluster, Interfacetype.MESSAGING) != null) {
            validateScheme(cluster, Interfacetype.MESSAGING);
        }
        if (CatalogServiceFactory.isEnabled()
                && ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY) != null) {
            validateScheme(cluster, Interfacetype.REGISTRY);
        }

        validateACL(cluster);

        if (!EntityUtil.responsibleFor(cluster.getColo())) {
            return;
        }

        validateReadInterface(cluster);
        validateWriteInterface(cluster);
        validateExecuteInterface(cluster);
        validateWorkflowInterface(cluster);
        validateMessagingInterface(cluster);
        validateRegistryInterface(cluster);
        validateLocations(cluster);
        validateProperties(cluster);
    }

    private void validateScheme(Cluster cluster, Interfacetype interfacetype)
        throws ValidationException {
        final String endpoint = ClusterHelper.getInterface(cluster, interfacetype).getEndpoint();
        URI uri = new Path(endpoint).toUri();
        if (uri.getScheme() == null) {
            if (Interfacetype.WORKFLOW == interfacetype
                    && uri.toString().equals(OozieConstants.LOCAL_OOZIE)) {
                return;
            }
            throw new ValidationException("Cannot get valid scheme for interface: "
                    + interfacetype + " of cluster: " + cluster.getName());
        }
    }

    private void validateReadInterface(Cluster cluster) throws ValidationException {
        final String readOnlyStorageUrl = ClusterHelper.getReadOnlyStorageUrl(cluster);
        LOG.info("Validating read interface: {}", readOnlyStorageUrl);

        validateFileSystem(cluster, readOnlyStorageUrl);
    }

    private void validateWriteInterface(Cluster cluster) throws ValidationException {
        final String writeStorageUrl = ClusterHelper.getStorageUrl(cluster);
        LOG.info("Validating write interface: {}", writeStorageUrl);

        validateFileSystem(cluster, writeStorageUrl);
    }

    private void validateFileSystem(Cluster cluster, String storageUrl) throws ValidationException {
        try {
            Configuration conf = new Configuration();
            conf.set(HadoopClientFactory.FS_DEFAULT_NAME_KEY, storageUrl);
            conf.setInt("ipc.client.connect.max.retries", 10);

            if (UserGroupInformation.isSecurityEnabled()) {
                String nameNodePrincipal = ClusterHelper.getPropertyValue(cluster, SecurityUtil.NN_PRINCIPAL);
                Validate.notEmpty(nameNodePrincipal,
                    "Cluster definition missing required namenode credential property: " + SecurityUtil.NN_PRINCIPAL);

                conf.set(SecurityUtil.NN_PRINCIPAL, nameNodePrincipal);
            }

            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);
            fs.exists(new Path("/"));
        } catch (Exception e) {
            throw new ValidationException("Invalid storage server or port: " + storageUrl
                    + ", " + e.getMessage(), e);
        }
    }

    private void validateExecuteInterface(Cluster cluster) throws ValidationException {
        String executeUrl = ClusterHelper.getMREndPoint(cluster);
        LOG.info("Validating execute interface: {}", executeUrl);

        try {
            HadoopClientFactory.get().validateJobClient(executeUrl);
        } catch (IOException e) {
            throw new ValidationException("Invalid Execute server or port: " + executeUrl, e);
        }
    }

    protected void validateWorkflowInterface(Cluster cluster) throws ValidationException {
        final String workflowUrl = ClusterHelper.getOozieUrl(cluster);
        LOG.info("Validating workflow interface: {}", workflowUrl);
        if (OozieConstants.LOCAL_OOZIE.equals(workflowUrl)) {
            return;
        }
        try {
            if (!WorkflowEngineFactory.getWorkflowEngine().isAlive(cluster)) {
                throw new ValidationException("Unable to reach Workflow server:" + workflowUrl);
            }
        } catch (FalconException e) {
            throw new ValidationException("Invalid Workflow server or port: " + workflowUrl, e);
        }
    }

    protected void validateMessagingInterface(Cluster cluster) throws ValidationException {
        // Validate only if user has specified this
        final Interface messagingInterface = ClusterHelper.getInterface(cluster, Interfacetype.MESSAGING);
        if (messagingInterface == null) {
            LOG.info("Messaging service is not enabled for cluster: {}", cluster.getName());
            return;
        }

        final String messagingUrl = ClusterHelper.getMessageBrokerUrl(cluster);
        final String implementation = StartupProperties.get().getProperty("broker.impl.class",
                "org.apache.activemq.ActiveMQConnectionFactory");
        LOG.info("Validating messaging interface: {}, implementation: {}", messagingUrl, implementation);

        try {
            @SuppressWarnings("unchecked")
            Class<ConnectionFactory> clazz = (Class<ConnectionFactory>)
                    getClass().getClassLoader().loadClass(implementation);
            ConnectionFactory connectionFactory = clazz.getConstructor(
                    String.class, String.class, String.class).newInstance("", "", messagingUrl);
            connectionFactory.createConnection();
        } catch (Exception e) {
            throw new ValidationException("Invalid Messaging server or port: " + messagingUrl
                    + " for: " + implementation, e);
        }
    }

    protected void validateRegistryInterface(Cluster cluster) throws ValidationException {
        final boolean isCatalogRegistryEnabled = CatalogServiceFactory.isEnabled();
        if (!isCatalogRegistryEnabled) {
            return;  // ignore the registry interface for backwards compatibility
        }

        // continue validation only if a catalog service is provided
        final Interface catalogInterface = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        if (catalogInterface == null) {
            LOG.info("Catalog service is not enabled for cluster: {}", cluster.getName());
            return;
        }

        final String catalogUrl = catalogInterface.getEndpoint();
        LOG.info("Validating catalog registry interface: {}", catalogUrl);

        try {
            Configuration clusterConf = ClusterHelper.getConfiguration(cluster);
            if (UserGroupInformation.isSecurityEnabled()) {
                String metaStorePrincipal = clusterConf.get(SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
                Validate.notEmpty(metaStorePrincipal,
                        "Cluster definition missing required metastore credential property: "
                                + SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
            }

            if (!CatalogServiceFactory.getCatalogService().isAlive(clusterConf, catalogUrl)) {
                throw new ValidationException("Unable to reach Catalog server:" + catalogUrl);
            }
        } catch (FalconException e) {
            throw new ValidationException("Invalid Catalog server or port: " + catalogUrl, e);
        }
    }

    /**
     * Validate ACL if authorization is enabled.
     *
     * @param cluster cluster entity
     * @throws ValidationException
     */
    private void validateACL(Cluster cluster) throws ValidationException {
        if (isAuthorizationDisabled) {
            return;
        }

        // Validate the entity owner is logged-in, authenticated user if authorization is enabled
        final ACL clusterACL = cluster.getACL();
        if (clusterACL == null) {
            throw new ValidationException("Cluster ACL cannot be empty for:  " + cluster.getName());
        }

        validateACLOwnerAndGroup(clusterACL);

        try {
            authorize(cluster.getName(), clusterACL);
        } catch (AuthorizationException e) {
            throw new ValidationException(e);
        }
    }

    /**
     * Validate the locations on the cluster exists with appropriate permissions
     * for the user to write to this directory.
     *
     * @param cluster cluster entity
     * @throws ValidationException
     */
    protected void validateLocations(Cluster cluster) throws ValidationException {
        Configuration conf = ClusterHelper.getConfiguration(cluster);
        FileSystem fs;
        try {
            fs = HadoopClientFactory.get().createFalconFileSystem(conf);
        } catch (FalconException e) {
            throw new ValidationException("Unable to get file system handle for cluster " + cluster.getName(), e);
        }

        Location stagingLocation = ClusterHelper.getLocation(cluster, ClusterLocationType.STAGING);
        if (stagingLocation == null) {
            throw new ValidationException(
                    "Unable to find the mandatory location of name: " + ClusterLocationType.STAGING.value()
                            + " for cluster " + cluster.getName());
        } else {
            checkPathOwnerAndPermission(cluster.getName(), stagingLocation.getPath(), fs,
                    HadoopClientFactory.ALL_PERMISSION);
            if (!ClusterHelper.checkWorkingLocationExists(cluster)) {
                //Creating location type of working in the sub dir of staging dir with perms 755. FALCON-910
                createWorkingDirUnderStaging(fs, cluster, stagingLocation);
            } else {
                Location workingLocation = ClusterHelper.getLocation(cluster, ClusterLocationType.WORKING);
                if (stagingLocation.getPath().equals(workingLocation.getPath())) {
                    throw new ValidationException(
                            "Location with name: " + stagingLocation.getName().value() + " and " + workingLocation
                                    .getName().value() + " cannot have same path: " + stagingLocation.getPath()
                                    + " for cluster :" + cluster.getName());
                } else {
                    checkPathOwnerAndPermission(cluster.getName(), workingLocation.getPath(), fs,
                            HadoopClientFactory.READ_EXECUTE_PERMISSION);
                }
            }
            // Create staging subdirs falcon/workflows/feed and falcon/workflows/process : Falcon-1647
            createStagingSubdirs(fs, cluster, stagingLocation,
                    "falcon/workflows/feed", HadoopClientFactory.ALL_PERMISSION);
            createStagingSubdirs(fs, cluster, stagingLocation,
                    "falcon/workflows/process", HadoopClientFactory.ALL_PERMISSION);

            // Create empty dirs for optional input
            createStagingSubdirs(fs, cluster, stagingLocation,
                    ClusterHelper.EMPTY_DIR_NAME, HadoopClientFactory.READ_ONLY_PERMISSION);
        }
    }

    private void createWorkingDirUnderStaging(FileSystem fs, Cluster cluster,
                                              Location stagingLocation) throws ValidationException {
        Path workingDirPath = new Path(stagingLocation.getPath(), ClusterHelper.WORKINGDIR);
        try {
            if (!fs.exists(workingDirPath)) {  //Checking if the staging dir has the working dir to be created
                HadoopClientFactory.mkdirs(fs, workingDirPath, HadoopClientFactory.READ_EXECUTE_PERMISSION);
            } else {
                if (fs.isDirectory(workingDirPath)) {
                    FsPermission workingPerms = fs.getFileStatus(workingDirPath).getPermission();
                    if (!workingPerms.equals(HadoopClientFactory.READ_EXECUTE_PERMISSION)) { //perms check
                        throw new ValidationException(
                                "Falcon needs subdir " + ClusterHelper.WORKINGDIR + " inside staging dir:"
                                        + stagingLocation.getPath()
                                        + " when staging location not specified with "
                                        + HadoopClientFactory.READ_EXECUTE_PERMISSION.toString() + " got "
                                        + workingPerms.toString());
                    }
                } else {
                    throw new ValidationException(
                            "Falcon needs subdir " + ClusterHelper.WORKINGDIR + " inside staging dir:"
                                    + stagingLocation.getPath()
                                    + " when staging location not specified. Got a file at " + workingDirPath
                                    .toString());
                }
            }
        } catch (IOException e) {
            throw new ValidationException(
                    "Unable to create path for " + workingDirPath.toString() + " with path: "
                            + workingDirPath.toString() + " for cluster " + cluster.getName(), e);
        }
    }

    private void createStagingSubdirs(FileSystem fs, Cluster cluster, Location stagingLocation,
                                      String path, FsPermission permission) throws ValidationException {
        Path subdirPath = new Path(stagingLocation.getPath(), path);
        try {
            HadoopClientFactory.mkdirs(fs, subdirPath, permission);
        } catch (IOException e) {
            throw new ValidationException(
                    "Unable to create path "
                            + subdirPath.toString() + " for cluster " + cluster.getName(), e);
        }
    }

    protected void validateProperties(Cluster cluster) throws ValidationException {
        Properties properties = cluster.getProperties();
        if (properties == null) {
            return; // Cluster has no properties to validate.
        }

        List<Property> propertyList = cluster.getProperties().getProperties();
        HashSet<String> propertyKeys = new HashSet<String>();
        for (Property prop : propertyList) {
            if (StringUtils.isBlank(prop.getName())) {
                throw new ValidationException("Property name and value cannot be empty for Cluster: "
                        + cluster.getName());
            }
            if (!propertyKeys.add(prop.getName())) {
                throw new ValidationException("Multiple properties with same name found for Cluster: "
                        + cluster.getName());
            }
        }
    }

    private void checkPathOwnerAndPermission(String clusterName, String location, FileSystem fs,
            FsPermission expectedPermission) throws ValidationException {

        Path locationPath = new Path(location);
        try {
            if (!fs.exists(locationPath)) {
                throw new ValidationException("Location " + location + " for cluster " + clusterName + " must exist.");
            }

            // falcon owns this path on each cluster
            final String loginUser = UserGroupInformation.getLoginUser().getShortUserName();
            FileStatus fileStatus = fs.getFileStatus(locationPath);
            final String locationOwner = fileStatus.getOwner();
            if (!locationOwner.equals(loginUser)) {
                LOG.error("Owner of the location {} is {} for cluster {}. Current user {} is not the owner of the "
                        + "location.", locationPath, locationOwner, clusterName, loginUser);
                throw new ValidationException("Path [" + locationPath + "] on the cluster [" + clusterName + "] has "
                        + "owner [" + locationOwner + "]. Current user [" + loginUser + "] is not the owner of the "
                        + "path");
            }
            String errorMessage = "Path " + locationPath + " has permissions: " + fileStatus.getPermission().toString()
                    + ", should be " + expectedPermission;
            if (fileStatus.getPermission().toShort() != expectedPermission.toShort()) {
                LOG.error(errorMessage);
                throw new ValidationException(errorMessage);
            }
            // try to list to see if the user is able to write to this folder
            fs.listStatus(locationPath);
        } catch (IOException e) {
            throw new ValidationException(
                    "Unable to validate the location with path: " + location + " for cluster:" + clusterName
                            + " due to transient failures ", e);
        }
    }
}
