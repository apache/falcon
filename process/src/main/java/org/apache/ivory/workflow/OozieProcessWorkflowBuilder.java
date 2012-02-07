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

package org.apache.ivory.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.mappers.CoordinatorMapper;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class OozieProcessWorkflowBuilder extends WorkflowBuilder {

    private static Logger LOG = Logger.getLogger(OozieProcessWorkflowBuilder.class);

    private final Marshaller marshaller;

    private static final ConfigurationStore configStore = ConfigurationStore.get();
    public static final String NAME_NODE = "nameNode";
    public static final String JOB_TRACKER = "jobTracker";

    public OozieProcessWorkflowBuilder() throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(COORDINATORAPP.class);
        this.marshaller = jaxbContext.createMarshaller();
    }

    @Override
    public Map<String, Object> newWorkflowSchedule(Entity entity)
            throws IvoryException {
        if (!(entity instanceof Process))
            throw new IllegalArgumentException(entity.getName() +
                    " is not of type Process");

        Process process = (Process) entity;
        //TODO asserts
        String clusterName = process.getClusters().getCluster().get(0).getName();
        Cluster cluster = configStore.get(EntityType.CLUSTER, clusterName);
        Path workflowPath = new Path(ClusterHelper.
                getLocation(cluster, "staging"), "workflows/process");
        COORDINATORAPP coordinatorApp = mapToCoordinator(process);
        Path path = new Path(workflowPath, "IVORY_PROCESS_" +
                process.getName() + ".xml");
        try {
            marshallToHDFS(coordinatorApp, path);
            return createAppProperties(cluster, path);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new IvoryException(e);
        }
    }

    @Override
    public Cluster[] getScheduledClustersFor(Entity entity)
            throws IvoryException{

        if (!(entity instanceof Process))
            throw new IllegalArgumentException(entity.getName() +
                    " is not of type Process");

        Process process = (Process) entity;
        //TODO asserts
        String clusterName = process.getClusters().getCluster().get(0).getName();
        Cluster cluster = configStore.get(EntityType.CLUSTER, clusterName);
        return new Cluster[] {cluster};
    }

    private Map<String, Object> createAppProperties(Cluster cluster, Path path)
            throws IvoryException {
        Properties properties = new Properties();
        properties.setProperty(NAME_NODE, ClusterHelper.getHdfsUrl(cluster));
        properties.setProperty(JOB_TRACKER, ClusterHelper.getMREndPoint(cluster));
        properties.setProperty(OozieClient.COORDINATOR_APP_PATH, path.toString());
        properties.setProperty(OozieClient.USER_NAME, StartupProperties.
                get().getProperty("oozie.user.name"));
        //TODO User name is hacked for now.
        Map<String, Object> map = new HashMap<String, Object>();
        List<Properties> props = new ArrayList<Properties>();
        List<Cluster> clusters = new ArrayList<Cluster>();

        props.add(properties);
        clusters.add(cluster);

        map.put(PROPS, props);
        map.put(CLUSTERS, clusters);
        return map;
    }

    private COORDINATORAPP mapToCoordinator(Process process) throws IvoryException {
        Map<Entity, EntityType> entityMap = new LinkedHashMap<Entity, EntityType>();

        entityMap.put(process, EntityType.PROCESS);

        for (Input input : process.getInputs().getInput()) {
            Feed dataset = configStore.get(EntityType.FEED, input.getFeed());
            assert dataset != null : "No valid dataset found for " + input.getFeed();
            entityMap.put(dataset, EntityType.FEED);
        }

        for (Output output : process.getOutputs().getOutput()) {
            Feed dataset = configStore.get(EntityType.FEED, output.getFeed());
            assert dataset != null : "No valid dataset found for " + output.getFeed();
            entityMap.put(dataset, EntityType.FEED);
        }

        COORDINATORAPP coordinatorApp = new COORDINATORAPP();
        CoordinatorMapper coordinatorMapper = new CoordinatorMapper(entityMap, coordinatorApp);
        coordinatorMapper.mapToDefaultCoordinator();
        LOG.info("Mapped to default coordinator");
        coordinatorApp.setName("IVORY_PROCESS_" + process.getName());
        return coordinatorApp;
    }

    private void marshallToHDFS(COORDINATORAPP coordinatorApp, Path path) throws IOException, IvoryException {
        ObjectFactory coordinatorObjectFactory = new ObjectFactory();
        JAXBElement<COORDINATORAPP> jaxbCoordinatorApp = coordinatorObjectFactory.createCoordinatorApp(coordinatorApp);

        FileSystem fs = path.getFileSystem(new Configuration());
        OutputStream outStream = fs.create(path);
        try {
            marshaller.marshal(jaxbCoordinatorApp, outStream);
        } catch (JAXBException e) {
            LOG.error(e.getMessage());
            throw new IvoryException("Unable to create oozie coordinator app", e);
        } finally {
            outStream.close();
        }
    }
}
