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

import java.io.IOException;

import javax.jms.ConnectionFactory;

import org.apache.commons.lang.Validate;
import org.apache.falcon.FalconException;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.StoreAccessException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser that parses cluster entity definition.
 */
public class ClusterEntityParser extends EntityParser<Cluster> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessEntityParser.class);

    public ClusterEntityParser() {
        super(EntityType.CLUSTER);
    }

    @Override
    public void validate(Cluster cluster) throws StoreAccessException, ValidationException {
        // validating scheme in light of fail-early
        validateScheme(cluster, Interfacetype.READONLY);
        validateScheme(cluster, Interfacetype.WRITE);
        validateScheme(cluster, Interfacetype.WORKFLOW);
        validateScheme(cluster, Interfacetype.MESSAGING);
        if (CatalogServiceFactory.isEnabled()
                && ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY) != null) {
            validateScheme(cluster, Interfacetype.REGISTRY);
        }

        if (!EntityUtil.responsibleFor(cluster.getColo())) {
            return;
        }

        validateReadInterface(cluster);
        validateWriteInterface(cluster);
        validateExecuteInterface(cluster);
        validateWorkflowInterface(cluster);
        validateMessagingInterface(cluster);
        validateRegistryInterface(cluster);
    }

    private void validateScheme(Cluster cluster, Interfacetype interfacetype)
        throws ValidationException {
        final String endpoint = ClusterHelper.getInterface(cluster, interfacetype).getEndpoint();
        if (new Path(endpoint).toUri().getScheme() == null) {
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

            // todo: ideally check if the end user has access using createProxiedFileSystem
            // hftp won't work and bug is logged at HADOOP-10215
            HadoopClientFactory.get().createFileSystem(conf);
        } catch (FalconException e) {
            throw new ValidationException("Invalid storage server or port: " + storageUrl, e);
        }
    }

    private void validateExecuteInterface(Cluster cluster) throws ValidationException {
        String executeUrl = ClusterHelper.getMREndPoint(cluster);
        LOG.info("Validating execute interface: {}", executeUrl);

        try {
            HadoopClientFactory.validateJobClient(executeUrl);
        } catch (IOException e) {
            throw new ValidationException("Invalid Execute server or port: " + executeUrl, e);
        }
    }

    private void validateWorkflowInterface(Cluster cluster) throws ValidationException {
        final String workflowUrl = ClusterHelper.getOozieUrl(cluster);
        LOG.info("Validating workflow interface: {}", workflowUrl);

        try {
            if (!WorkflowEngineFactory.getWorkflowEngine().isAlive(cluster)) {
                throw new ValidationException("Unable to reach Workflow server:" + workflowUrl);
            }
        } catch (FalconException e) {
            throw new ValidationException("Invalid Workflow server or port: " + workflowUrl, e);
        }
    }

    private void validateMessagingInterface(Cluster cluster) throws ValidationException {
        final String messagingUrl = ClusterHelper.getMessageBrokerUrl(cluster);
        final String implementation = StartupProperties.get().getProperty(
                "broker.impl.class", "org.apache.activemq.ActiveMQConnectionFactory");
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

    private void validateRegistryInterface(Cluster cluster) throws ValidationException {
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
            String metaStorePrincipal = null;
            if (UserGroupInformation.isSecurityEnabled()) {
                metaStorePrincipal = ClusterHelper.getPropertyValue(cluster, SecurityUtil.HIVE_METASTORE_PRINCIPAL);
                Validate.notEmpty(metaStorePrincipal,
                        "Cluster definition missing required metastore credential property: "
                                + SecurityUtil.HIVE_METASTORE_PRINCIPAL);
            }

            if (!CatalogServiceFactory.getCatalogService().isAlive(catalogUrl, metaStorePrincipal)) {
                throw new ValidationException("Unable to reach Catalog server:" + catalogUrl);
            }
        } catch (FalconException e) {
            throw new ValidationException("Invalid Catalog server or port: " + catalogUrl, e);
        }
    }
}
