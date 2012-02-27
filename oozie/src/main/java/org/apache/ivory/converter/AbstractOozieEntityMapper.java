/*
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

package org.apache.ivory.converter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Pair;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.oozie.bundle.BUNDLEAPP;
import org.apache.ivory.oozie.bundle.COORDINATOR;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.OutputStream;
import java.util.List;

public abstract class AbstractOozieEntityMapper<T extends Entity> {

    private static Logger LOG = Logger.getLogger(AbstractOozieEntityMapper.class);

    public static final String NAME_NODE = "nameNode";
    public static final String JOB_TRACKER = "jobTracker";

    private static final JAXBContext coordJaxbContext;
    private static final JAXBContext bundleJaxbContext;

    static {
        try {
            coordJaxbContext = JAXBContext.newInstance(COORDINATORAPP.class);
            bundleJaxbContext = JAXBContext.newInstance(BUNDLEAPP.class);
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXB context", e);
        }
    }

    protected static final ConfigurationStore configStore = ConfigurationStore.get();

    private final T entity;

    protected AbstractOozieEntityMapper(T entity) {
        this.entity = entity;
    }

    protected T getEntity() {
        return entity;
    }

    public Path convert(Path workflowBasePath) throws IvoryException {
        BUNDLEAPP bundleApp = new BUNDLEAPP();
        bundleApp.setName(entity.getWorkflowName());

        Pair<Cluster, List<COORDINATORAPP>> coordinators = getCoordinators();

        for (COORDINATORAPP coordinatorapp : coordinators.second) {
            Path coordPath = new Path(workflowBasePath,
                    coordinatorapp.getName() + "/coordinator.xml");
            marshal(coordinators.first, coordinatorapp, coordPath);
            COORDINATOR bundleCoord = new COORDINATOR();
            bundleCoord.setName(coordinatorapp.getName());
            bundleCoord.setAppPath("${" + NAME_NODE + "}" + coordPath);
            bundleCoord.setConfiguration(createBundleConf());
            bundleApp.getCoordinator().add(bundleCoord);
        }
        Path bundlePath = new Path(workflowBasePath,
                bundleApp.getName() + "/bundle.xml");
        marshal(coordinators.first, bundleApp, bundlePath);
        return bundlePath;
    }

    protected abstract Pair<Cluster, List<COORDINATORAPP>> getCoordinators()
            throws IvoryException;

    protected org.apache.ivory.oozie.bundle.CONFIGURATION createBundleConf() {
        org.apache.ivory.oozie.bundle.CONFIGURATION conf = new org.apache.ivory.oozie.bundle.CONFIGURATION();
        conf.getProperty().add(createBundleProperty(NAME_NODE, "${" + NAME_NODE + "}"));
        conf.getProperty().add(createBundleProperty(JOB_TRACKER, "${" + JOB_TRACKER + "}"));
        return conf;
    }

    protected org.apache.ivory.oozie.coordinator.CONFIGURATION.Property createCoordProperty(String name, String value) {
        org.apache.ivory.oozie.coordinator.CONFIGURATION.Property prop = new org.apache.ivory.oozie.coordinator.CONFIGURATION.Property();
        prop.setName(name);
        prop.setValue(value);
        return prop;
    }

    protected org.apache.ivory.oozie.bundle.CONFIGURATION.Property createBundleProperty(String name, String value) {
        org.apache.ivory.oozie.bundle.CONFIGURATION.Property prop = new org.apache.ivory.oozie.bundle.CONFIGURATION.Property();
        prop.setName(name);
        prop.setValue(value);
        return prop;
    }

    protected void marshal(Cluster cluster, JAXBElement<?> jaxbElement,
                             JAXBContext jaxbContext,
                             Path outPath) throws IvoryException {
        try{
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            FileSystem fs = outPath.getFileSystem(ClusterHelper.
                    getConfiguration(cluster));
            OutputStream out = fs.create(outPath);
            try {
                marshaller.marshal(jaxbElement, out);
            } finally {
                out.close();
            }
            LOG.info("Marshalled " + jaxbElement.getDeclaredType() + " to " + outPath);
        } catch(Exception e) {
            throw new IvoryException("Unable to marshall app object", e);
        }
    }

    protected void marshal(Cluster cluster, COORDINATORAPP coord, Path outPath)
            throws IvoryException {

        marshal(cluster, new ObjectFactory().createCoordinatorApp(coord),
                coordJaxbContext, outPath);
    }

    protected void marshal(Cluster cluster, BUNDLEAPP bundle, Path outPath)
            throws IvoryException {

        marshal(cluster, new org.apache.ivory.oozie.bundle.ObjectFactory().
                createBundleApp(bundle), bundleJaxbContext, outPath);
    }
}
