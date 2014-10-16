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

package org.apache.falcon.entity;

import org.apache.commons.io.FileUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeClass;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

/**
 * Base class for config store test.
 */
public class AbstractTestBase {
    protected static final String USER = System.getProperty("user.name");

    protected static final String PROCESS_XML = "/config/process/process-0.1.xml";
    protected static final String FEED_XML = "/config/feed/feed-0.1.xml";
    protected static final String CLUSTER_XML = "/config/cluster/cluster-0.1.xml";
    protected EmbeddedCluster dfsCluster;
    protected Configuration conf = new Configuration();
    private ConfigurationStore store;

    public ConfigurationStore getStore() {
        return store;
    }

    @BeforeClass
    public void initConfigStore() throws Exception {
        String configPath = new URI(StartupProperties.get().getProperty("config.store.uri")).getPath();
        String location = configPath + "-" + getClass().getName();
        StartupProperties.get().setProperty("config.store.uri", location);
        FileUtils.deleteDirectory(new File(location));

        cleanupStore();
        String listeners = StartupProperties.get().getProperty("configstore.listeners");
        StartupProperties.get().setProperty("configstore.listeners",
                listeners.replace("org.apache.falcon.service.SharedLibraryHostingService", ""));
        store = ConfigurationStore.get();
        store.init();

        CurrentUser.authenticate("testuser");
    }

    protected void cleanupStore() throws FalconException {
        store = ConfigurationStore.get();
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for (String entity : entities) {
                store.remove(type, entity);
            }
        }
    }

    protected void storeEntity(EntityType type, String name) throws Exception {
        final String proxyUser = CurrentUser.getUser();
        final String defaultGroupName = CurrentUser.getPrimaryGroupName();

        Unmarshaller unmarshaller = type.getUnmarshaller();
        store = ConfigurationStore.get();
        store.remove(type, name);
        switch (type) {
        case CLUSTER:
            Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
            cluster.setName(name);
            ClusterHelper.getInterface(cluster, Interfacetype.WRITE)
                    .setEndpoint(conf.get(HadoopClientFactory.FS_DEFAULT_NAME_KEY));
            decorateACL(proxyUser, defaultGroupName, cluster);

            store.publish(type, cluster);
            break;

        case FEED:
            Feed feed = (Feed) unmarshaller.unmarshal(this.getClass().getResource(FEED_XML));
            feed.setName(name);
            decorateACL(proxyUser, defaultGroupName, feed);

            store.publish(type, feed);
            break;

        case PROCESS:
            Process process = (Process) unmarshaller.unmarshal(this.getClass().getResource(PROCESS_XML));
            process.setName(name);
            FileSystem fs = dfsCluster.getFileSystem();
            fs.mkdirs(new Path(process.getWorkflow().getPath()));
            if (!fs.exists(new Path(process.getWorkflow() + "/lib"))) {
                fs.mkdirs(new Path(process.getWorkflow() + "/lib"));
            }

            decorateACL(proxyUser, defaultGroupName, process);

            store.publish(type, process);
            break;
        default:
        }
    }

    private void decorateACL(String proxyUser, String defaultGroupName, Cluster cluster) {
        if (cluster.getACL() != null) {
            return;
        }

        org.apache.falcon.entity.v0.cluster.ACL clusterACL =
                new org.apache.falcon.entity.v0.cluster.ACL();
        clusterACL.setOwner(proxyUser);
        clusterACL.setGroup(defaultGroupName);
        cluster.setACL(clusterACL);
    }

    private void decorateACL(String proxyUser, String defaultGroupName, Feed feed) {
        if (feed.getACL() != null) {
            return;
        }

        org.apache.falcon.entity.v0.feed.ACL feedACL =
                new org.apache.falcon.entity.v0.feed.ACL();
        feedACL.setOwner(proxyUser);
        feedACL.setGroup(defaultGroupName);
        feed.setACL(feedACL);
    }

    private void decorateACL(String proxyUser, String defaultGroupName,
                             Process process) {
        if (process.getACL() != null) {
            return;
        }

        org.apache.falcon.entity.v0.process.ACL processACL =
                new org.apache.falcon.entity.v0.process.ACL();
        processACL.setOwner(proxyUser);
        processACL.setGroup(defaultGroupName);
        process.setACL(processACL);
    }

    public void setup() throws Exception {
        store = ConfigurationStore.get();
        for (EntityType type : EntityType.values()) {
            for (String name : store.getEntities(type)) {
                store.remove(type, name);
            }
        }
        storeEntity(EntityType.CLUSTER, "corp");
        storeEntity(EntityType.FEED, "clicks");
        storeEntity(EntityType.FEED, "impressions");
        storeEntity(EntityType.FEED, "clicksummary");
        storeEntity(EntityType.PROCESS, "clicksummary");
    }

    public String marshallEntity(final Entity entity) throws FalconException,
                                                             JAXBException {
        Marshaller marshaller = entity.getEntityType().getMarshaller();
        StringWriter stringWriter = new StringWriter();
        marshaller.marshal(entity, stringWriter);
        return stringWriter.toString();
    }

    // assumes there will always be at least one group for a logged in user
    protected String getGroupName() throws IOException {
        String[] groupNames = CurrentUser.getProxyUGI().getGroupNames();
        System.out.println("groupNames = " + Arrays.asList(groupNames));
        return groupNames[0];
    }
}
