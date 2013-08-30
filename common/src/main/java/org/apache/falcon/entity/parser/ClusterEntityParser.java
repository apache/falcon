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

import org.apache.falcon.FalconException;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.store.StoreAccessException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
/**
 * Parser that parses cluster entity definition.
 */
public class ClusterEntityParser extends EntityParser<Cluster> {

    private static final Logger LOG = Logger.getLogger(ProcessEntityParser.class);

    public ClusterEntityParser() {
        super(EntityType.CLUSTER);
    }

    @Override
    public void validate(Cluster cluster) throws StoreAccessException,
                                                 ValidationException {
        // validating scheme in light of fail-early
        validateScheme(cluster, Interfacetype.READONLY);
        validateScheme(cluster, Interfacetype.WRITE);
        validateScheme(cluster, Interfacetype.WORKFLOW);
        validateScheme(cluster, Interfacetype.MESSAGING);
        if (ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY) != null) {
            validateScheme(cluster, Interfacetype.REGISTRY);
        }

        // No interface validations in prism or other falcon servers.
        // Only the falcon server for which the cluster belongs to should validate interfaces
        if (DeploymentUtil.isPrism() || !cluster.getColo().equals(DeploymentUtil.getCurrentColo())) {
            LOG.info("No interface validations in prism or falcon servers not applicable.");
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
        LOG.info("Validating read interface: " + readOnlyStorageUrl);

        validateFileSystem(readOnlyStorageUrl);
    }

    private void validateWriteInterface(Cluster cluster) throws ValidationException {
        final String writeStorageUrl = ClusterHelper.getStorageUrl(cluster);
        LOG.info("Validating write interface: " + writeStorageUrl);

        validateFileSystem(writeStorageUrl);
    }

    private void validateFileSystem(String storageUrl) throws ValidationException {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.default.name", storageUrl);
            conf.setInt("ipc.client.connect.max.retries", 10);
            FileSystem.get(conf);
        } catch (IOException e) {
            throw new ValidationException("Invalid storage server or port: " + storageUrl, e);
        }
    }

    private void validateExecuteInterface(Cluster cluster) throws ValidationException {
        String executeUrl = ClusterHelper.getMREndPoint(cluster);
        LOG.info("Validating execute interface: " + executeUrl);

        try {
            JobConf jobConf = new JobConf();
            jobConf.set("mapred.job.tracker", executeUrl);
            jobConf.set("yarn.resourcemanager.address", executeUrl);
            JobClient jobClient = new JobClient(jobConf);
            jobClient.getClusterStatus().getMapTasks();
        } catch (IOException e) {
            throw new ValidationException("Invalid Execute server or port: " + executeUrl, e);
        }
    }

    private void validateWorkflowInterface(Cluster cluster) throws ValidationException {
        final String workflowUrl = ClusterHelper.getOozieUrl(cluster);
        LOG.info("Validating workflow interface: " + workflowUrl);

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
        LOG.info("Validating messaging interface: " + messagingUrl + ", implementation: " + implementation);

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
        final Interface catalogInterface = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        if (catalogInterface == null) {
            LOG.info("Catalog service is not enabled for cluster: " + cluster.getName());
            return;
        }

        if (!CatalogServiceFactory.isEnabled()) {
            throw new ValidationException("Catalog registry implementation is not defined: catalog.service.impl");
        }

        final String catalogUrl = catalogInterface.getEndpoint();
        LOG.info("Validating catalog registry interface: " + catalogUrl);

        try {
            if (!CatalogServiceFactory.getCatalogService().isAlive(catalogUrl)) {
                throw new ValidationException("Unable to reach Catalog server:" + catalogUrl);
            }
        } catch (FalconException e) {
            throw new ValidationException("Invalid Catalog server or port: " + catalogUrl, e);
        }
    }
}
