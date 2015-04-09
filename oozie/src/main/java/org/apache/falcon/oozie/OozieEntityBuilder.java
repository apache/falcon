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

package org.apache.falcon.oozie;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.feed.FeedBundleBuilder;
import org.apache.falcon.oozie.process.ProcessBundleBuilder;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.service.FalconPathFilter;
import org.apache.falcon.service.SharedLibraryHostingService;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Base class for building oozie entities - workflow, coordinator and bundle.
 * @param <T>
 */
public abstract class OozieEntityBuilder<T extends Entity> {
    public static final Logger LOG = LoggerFactory.getLogger(OozieEntityBuilder.class);

    public static final String METASTOREURIS = "hive.metastore.uris";
    public static final String METASTORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";
    public static final String METASTORE_USE_THRIFT_SASL = "hive.metastore.sasl.enabled";

    public static final String ENTITY_PATH = "ENTITY_PATH";
    public static final String ENTITY_NAME = "ENTITY_NAME";

    private static final FalconPathFilter FALCON_JAR_FILTER = new FalconPathFilter() {
        @Override
        public boolean accept(Path path) {
            String fileName = path.getName();
            if (fileName.startsWith("falcon")) {
                return true;
            }
            return false;
        }

        @Override
        public String getJarName(Path path) {
            String name = path.getName();
            if (name.endsWith(".jar")) {
                name = name.substring(0, name.indexOf(".jar"));
            }
            return name;
        }
    };

    protected T entity;
    protected final boolean isSecurityEnabled = SecurityUtil.isSecurityEnabled();

    public OozieEntityBuilder(T entity) {
        this.entity = entity;
    }

    public abstract Properties build(Cluster cluster, Path buildPath) throws FalconException;

    protected String getStoragePath(Path path) {
        if (path != null) {
            return getStoragePath(path.toString());
        }
        return null;
    }

    protected String getStoragePath(String path) {
        if (StringUtils.isNotEmpty(path)) {
            if (new Path(path).toUri().getScheme() == null && !path.startsWith("${nameNode}")) {
                path = "${nameNode}" + path;
            }
        }
        return path;
    }

    public static OozieEntityBuilder get(Entity entity) {
        switch(entity.getEntityType()) {
        case FEED:
            return new FeedBundleBuilder((Feed) entity);

        case PROCESS:
            return new ProcessBundleBuilder((Process)entity);

        default:
        }
        throw new IllegalArgumentException("Unhandled type: " + entity.getEntityType());
    }

    protected Path marshal(Cluster cluster, JAXBElement<?> jaxbElement,
                           JAXBContext jaxbContext, Path outPath) throws FalconException {
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            if (LOG.isDebugEnabled()) {
                StringWriter writer = new StringWriter();
                marshaller.marshal(jaxbElement, writer);
                LOG.debug("Writing definition to {} on cluster {}", outPath, cluster.getName());
                LOG.debug(writer.getBuffer().toString());
            }

            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                    outPath.toUri(), ClusterHelper.getConfiguration(cluster));
            OutputStream out = fs.create(outPath);
            try {
                marshaller.marshal(jaxbElement, out);
            } finally {
                out.close();
            }

            LOG.info("Marshalled {} to {}", jaxbElement.getDeclaredType(), outPath);
            return outPath;
        } catch (Exception e) {
            throw new FalconException("Unable to marshall app object", e);
        }
    }

    protected Properties getHiveCredentials(Cluster cluster) {
        String metaStoreUrl = ClusterHelper.getRegistryEndPoint(cluster);
        if (metaStoreUrl == null) {
            throw new IllegalStateException(
                "Registry interface is not defined in cluster: " + cluster.getName());
        }

        Properties hiveCredentials = new Properties();
        hiveCredentials.put(METASTOREURIS, metaStoreUrl);
        hiveCredentials.put("hive.metastore.execute.setugi", "true");
        hiveCredentials.put("hcatNode", metaStoreUrl.replace("thrift", "hcat"));
        hiveCredentials.put("hcat.metastore.uri", metaStoreUrl);

        if (isSecurityEnabled) {
            String principal = ClusterHelper
                .getPropertyValue(cluster, SecurityUtil.HIVE_METASTORE_PRINCIPAL);
            hiveCredentials.put(METASTORE_KERBEROS_PRINCIPAL, principal);
            hiveCredentials.put(METASTORE_USE_THRIFT_SASL, "true");
            hiveCredentials.put("hcat.metastore.principal", principal);
        }

        return hiveCredentials;
    }

    protected Configuration getHiveCredentialsAsConf(Cluster cluster) {
        Properties hiveCredentials = getHiveCredentials(cluster);

        Configuration hiveConf = new Configuration(false);
        for (Entry<Object, Object> entry : hiveCredentials.entrySet()) {
            hiveConf.set((String)entry.getKey(), (String)entry.getValue());
        }

        return hiveConf;
    }

    protected Properties getEntityProperties(Entity myEntity) {
        Properties properties = new Properties();
        switch (myEntity.getEntityType()) {
        case CLUSTER:
            org.apache.falcon.entity.v0.cluster.Properties clusterProps = ((Cluster) myEntity).getProperties();
            if (clusterProps != null) {
                for (Property prop : clusterProps.getProperties()) {
                    properties.put(prop.getName(), prop.getValue());
                }
            }
            break;

        case FEED:
            org.apache.falcon.entity.v0.feed.Properties feedProps = ((Feed) myEntity).getProperties();
            if (feedProps != null) {
                for (org.apache.falcon.entity.v0.feed.Property prop : feedProps.getProperties()) {
                    properties.put(prop.getName(), prop.getValue());
                }
            }
            break;

        case PROCESS:
            org.apache.falcon.entity.v0.process.Properties processProps = ((Process) myEntity).getProperties();
            if (processProps != null) {
                for (org.apache.falcon.entity.v0.process.Property prop : processProps.getProperties()) {
                    properties.put(prop.getName(), prop.getValue());
                }
            }
            break;

        default:
            throw new IllegalArgumentException("Unhandled entity type " + myEntity.getEntityType());
        }
        return properties;
    }

    protected void propagateCatalogTableProperties(Output output, CatalogStorage tableStorage, Properties props) {
        String prefix = "falcon_" + output.getName();

        propagateCommonCatalogTableProperties(tableStorage, props, prefix);

        //pig and java actions require partition expression as "key1=val1, key2=val2"
        props.put(prefix + "_partitions_pig",
            "${coord:dataOutPartitions('" + output.getName() + "')}");
        props.put(prefix + "_partitions_java",
            "${coord:dataOutPartitions('" + output.getName() + "')}");

        //hive requires partition expression as "key1='val1', key2='val2'" (with quotes around values)
        //there is no direct EL expression in oozie
        List<String> partitions = new ArrayList<String>();
        for (String key : tableStorage.getDatedPartitionKeys()) {
            StringBuilder expr = new StringBuilder();
            expr.append("${coord:dataOutPartitionValue('").append(output.getName()).append("', '").append(key)
                .append("')}");
            props.put(prefix + "_dated_partition_value_" + key, expr.toString());
            partitions.add(key + "='" + expr + "'");

        }
        props.put(prefix + "_partitions_hive", StringUtils.join(partitions, ","));
    }

    protected void propagateCommonCatalogTableProperties(CatalogStorage tableStorage, Properties props, String prefix) {
        props.put(prefix + "_storage_type", tableStorage.getType().name());
        props.put(prefix + "_catalog_url", tableStorage.getCatalogUrl());
        props.put(prefix + "_database", tableStorage.getDatabase());
        props.put(prefix + "_table", tableStorage.getTable());
    }

    protected void copySharedLibs(Cluster cluster, Path libPath) throws FalconException {
        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                    libPath.toUri(), ClusterHelper.getConfiguration(cluster));
            SharedLibraryHostingService.pushLibsToHDFS(
                    fs, StartupProperties.get().getProperty("system.lib.location"),
                    libPath, FALCON_JAR_FILTER);
        } catch (IOException e) {
            throw new FalconException("Failed to copy shared libs on cluster " + cluster.getName(), e);
        }
    }

    protected Properties getProperties(Path path, String name) {
        if (path == null) {
            return null;
        }

        Properties prop = new Properties();
        prop.setProperty(OozieEntityBuilder.ENTITY_PATH, path.toString());
        prop.setProperty(OozieEntityBuilder.ENTITY_NAME, name);
        return prop;
    }

    protected <T> T unmarshal(String template, JAXBContext context, Class<T> cls) throws FalconException {
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = OozieEntityBuilder.class.getResourceAsStream(template);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            JAXBElement<T> jaxbElement = unmarshaller.unmarshal(new StreamSource(resourceAsStream), cls);
            return jaxbElement.getValue();
        } catch (JAXBException e) {
            throw new FalconException("Failed to unmarshal " + template, e);
        } finally {
            IOUtils.closeQuietly(resourceAsStream);
        }
    }
}
