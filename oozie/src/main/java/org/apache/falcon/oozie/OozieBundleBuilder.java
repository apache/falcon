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

import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.bundle.BUNDLEAPP;
import org.apache.falcon.oozie.bundle.CONFIGURATION;
import org.apache.falcon.oozie.bundle.CONFIGURATION.Property;
import org.apache.falcon.oozie.bundle.COORDINATOR;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Base class for building oozie bundle - bundle is the entity that falcon tracks in oozie.
 * @param <T>
 */
public abstract class OozieBundleBuilder<T extends Entity> extends OozieEntityBuilder<T> {
    public static final Logger LOG = LoggerFactory.getLogger(OozieBundleBuilder.class);

    public OozieBundleBuilder(T entity) {
        super(entity);
    }

    @Override public Properties build(Cluster cluster, Path buildPath) throws FalconException {
        String clusterName = cluster.getName();
        if (EntityUtil.getStartTime(entity, clusterName).compareTo(EntityUtil.getEndTime(entity, clusterName)) >= 0) {
            LOG.info("process validity start <= end for cluster {}. Skipping schedule", clusterName);
            return null;
        }

        List<Properties> coords = buildCoords(cluster, buildPath);
        if (coords == null || coords.isEmpty()) {
            return null;
        }

        BUNDLEAPP bundle = new BUNDLEAPP();
        bundle.setName(EntityUtil.getWorkflowName(entity).toString());
        // all the properties are set prior to bundle and coordinators creation

        for (Properties coordProps : coords) {
            // add the coordinator to the bundle
            COORDINATOR coord = new COORDINATOR();
            String coordPath = coordProps.getProperty(OozieEntityBuilder.ENTITY_PATH);
            final String coordName = coordProps.getProperty(OozieEntityBuilder.ENTITY_NAME);
            coord.setName(coordName);
            coord.setAppPath(getStoragePath(coordPath));
            Properties appProps = createAppProperties(cluster, buildPath, coordName);
            appProps.putAll(coordProps);
            coord.setConfiguration(getConfig(appProps));
            bundle.getCoordinator().add(coord);
        }

        Path marshalPath = marshal(cluster, bundle, buildPath); // write the bundle
        Properties properties = getProperties(marshalPath, entity.getName());
        properties.setProperty(OozieClient.BUNDLE_APP_PATH, getStoragePath(buildPath));
        properties.setProperty(AbstractWorkflowEngine.NAME_NODE, ClusterHelper.getStorageUrl(cluster));

        return properties;
    }

    protected CONFIGURATION getConfig(Properties props) {
        CONFIGURATION conf = new CONFIGURATION();
        for (Entry<Object, Object> prop : props.entrySet()) {
            Property confProp = new Property();
            confProp.setName((String) prop.getKey());
            confProp.setValue((String) prop.getValue());
            conf.getProperty().add(confProp);
        }
        return conf;
    }

    protected Properties createAppProperties(Cluster cluster, Path buildPath,
                                             String coordName) throws FalconException {
        Properties properties = getEntityProperties(cluster);
        properties.setProperty(AbstractWorkflowEngine.NAME_NODE, ClusterHelper.getStorageUrl(cluster));
        properties.setProperty(AbstractWorkflowEngine.JOB_TRACKER, ClusterHelper.getMREndPoint(cluster));
        properties.setProperty("colo.name", cluster.getColo());

        properties.setProperty(OozieClient.USER_NAME, CurrentUser.getUser());
        properties.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");
        properties.setProperty("falcon.libpath", ClusterHelper.getLocation(cluster, "working") + "/lib");

        if (EntityUtil.isTableStorageType(cluster, entity)) {
            Tag tag = EntityUtil.getWorkflowNameTag(coordName, entity);
            if (tag == Tag.REPLICATION) {
                // todo: kludge send source hcat creds for coord dependency check to pass
                String srcClusterName = EntityUtil.getWorkflowNameSuffix(coordName, entity);
                properties.putAll(getHiveCredentials(ClusterHelper.getCluster(srcClusterName)));
            } else {
                properties.putAll(getHiveCredentials(cluster));
            }
        }

        //Add libpath
        Path libPath = getLibPath(cluster, buildPath);
        if (libPath != null) {
            properties.put(OozieClient.LIBPATH, getStoragePath(libPath));
        }

        return properties;
    }

    protected Path marshal(Cluster cluster, BUNDLEAPP bundle, Path outPath) throws FalconException {
        return marshal(cluster, new org.apache.falcon.oozie.bundle.ObjectFactory().createBundleApp(bundle),
            OozieUtils.BUNDLE_JAXB_CONTEXT, new Path(outPath, "bundle.xml"));
    }

    //Used by coordinator builders to return multiple coords
    //TODO Can avoid separate interface that returns list by building at lifecycle level
    protected abstract List<Properties> buildCoords(Cluster cluster, Path bundlePath) throws FalconException;

    public static BUNDLEAPP unmarshal(Cluster cluster, Path path) throws FalconException {
        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                        path.toUri(), ClusterHelper.getConfiguration(cluster));
            Unmarshaller unmarshaller = OozieUtils.BUNDLE_JAXB_CONTEXT.createUnmarshaller();
            @SuppressWarnings("unchecked") JAXBElement<BUNDLEAPP> jaxbElement =
                    unmarshaller.unmarshal(new StreamSource(fs.open(path)), BUNDLEAPP.class);
            return jaxbElement.getValue();
        } catch (JAXBException e) {
            throw new FalconException(e);
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }
}
