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

package org.apache.falcon.oozie.process;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.oozie.bundle.BUNDLEAPP;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;

/**
 * Base for falcon unit tests involving configuration store.
 */
public class AbstractTestBase {
    protected Entity storeEntity(EntityType type, String name, String resource, String writeEndpoint) throws Exception {
        Unmarshaller unmarshaller = type.getUnmarshaller();
        ConfigurationStore store = ConfigurationStore.get();
        switch (type) {
        case CLUSTER:
            Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(resource));
            if (name != null){
                store.remove(type, name);
                cluster.setName(name);
            }
            store.publish(type, cluster);

            if (writeEndpoint != null) {
                ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(writeEndpoint);
                FileSystem fs = new Path(writeEndpoint).getFileSystem(EmbeddedCluster.newConfiguration());
                fs.create(
                    new Path(ClusterHelper.getLocation(cluster, "working"), "libext/FEED/retention/ext.jar")).close();
                fs.create(
                    new Path(ClusterHelper.getLocation(cluster, "working"), "libext/FEED/replication/ext.jar")).close();
            }

            return cluster;

        case FEED:
            Feed feed = (Feed) unmarshaller.unmarshal(this.getClass().getResource(resource));
            if (name != null) {
                store.remove(type, name);
                feed.setName(name);
            }
            store.publish(type, feed);
            return feed;

        case PROCESS:
            Process process = (Process) unmarshaller.unmarshal(this.getClass().getResource(resource));
            if (name != null) {
                store.remove(type, name);
                process.setName(name);
            }
            store.publish(type, process);
            return process;

        default:
        }

        throw new IllegalArgumentException("Unhandled type: " + type);
    }

    protected COORDINATORAPP getCoordinator(FileSystem fs, Path path) throws Exception {
        String coordStr = readFile(fs, path);

        Unmarshaller unmarshaller = JAXBContext.newInstance(COORDINATORAPP.class).createUnmarshaller();
        SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        Schema schema = schemaFactory.newSchema(this.getClass().getResource("/oozie-coordinator-0.3.xsd"));
        unmarshaller.setSchema(schema);
        JAXBElement<COORDINATORAPP> jaxbBundle = unmarshaller.unmarshal(
            new StreamSource(new ByteArrayInputStream(coordStr.trim().getBytes())), COORDINATORAPP.class);
        return jaxbBundle.getValue();
    }

    protected BUNDLEAPP getBundle(FileSystem fs, Path path) throws Exception {
        String bundleStr = readFile(fs, new Path(path, "bundle.xml"));

        Unmarshaller unmarshaller = JAXBContext.newInstance(BUNDLEAPP.class).createUnmarshaller();
        SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        Schema schema = schemaFactory.newSchema(this.getClass().getResource("/oozie-bundle-0.1.xsd"));
        unmarshaller.setSchema(schema);
        JAXBElement<BUNDLEAPP> jaxbBundle = unmarshaller.unmarshal(
            new StreamSource(new ByteArrayInputStream(bundleStr.trim().getBytes())), BUNDLEAPP.class);
        return jaxbBundle.getValue();
    }

    protected String readFile(FileSystem fs, Path path) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        StringBuilder contents = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            contents.append(line);
        }
        return contents.toString();
    }

    protected void cleanupStore() throws FalconException {
        ConfigurationStore store = ConfigurationStore.get();
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for (String entity : entities) {
                store.remove(type, entity);
            }
        }
    }

    protected void assertLibExtensions(FileSystem fs, COORDINATORAPP coord, EntityType type,
        String lifecycle) throws Exception {
        WORKFLOWAPP wf = getWorkflowapp(fs, coord);
        List<Object> actions = wf.getDecisionOrForkOrJoin();
        String lifeCyclePath = lifecycle == null ? "" : "/" + lifecycle;
        for (Object obj : actions) {
            if (!(obj instanceof ACTION)) {
                continue;
            }
            ACTION action = (ACTION) obj;
            List<String> files = null;
            if (action.getJava() != null) {
                files = action.getJava().getFile();
            } else if (action.getPig() != null) {
                files = action.getPig().getFile();
            } else if (action.getMapReduce() != null) {
                files = action.getMapReduce().getFile();
            }
            if (files != null) {
                Assert.assertTrue(files.get(files.size() - 1).endsWith(
                    "/projects/falcon/working/libext/" + type.name() + lifeCyclePath + "/ext.jar"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected WORKFLOWAPP getWorkflowapp(FileSystem fs, COORDINATORAPP coord) throws JAXBException, IOException {
        String wfPath = coord.getAction().getWorkflow().getAppPath().replace("${nameNode}", "");
        return getWorkflowapp(fs, new Path(wfPath));
    }

    @SuppressWarnings("unchecked")
    protected WORKFLOWAPP getWorkflowapp(FileSystem fs, Path path) throws JAXBException, IOException {
        JAXBContext jaxbContext = JAXBContext.newInstance(WORKFLOWAPP.class);
        return ((JAXBElement<WORKFLOWAPP>) jaxbContext.createUnmarshaller().unmarshal(fs.open(path))).getValue();
    }
}
