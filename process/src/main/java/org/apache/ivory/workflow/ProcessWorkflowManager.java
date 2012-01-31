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

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.dataset.Dataset;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.mappers.CoordinatorMapper;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;

public class ProcessWorkflowManager extends EntityWorkflowManager<Entity> {

    private static Logger LOG = Logger.getLogger(ProcessWorkflowManager.class);

    private final Marshaller marshaller;

    private static final ConfigurationStore configStore = ConfigurationStore.get();

    public ProcessWorkflowManager() throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(COORDINATORAPP.class);
        this.marshaller = jaxbContext.createMarshaller();
    }

    @Override
    public String schedule(Entity process) throws IvoryException {

        COORDINATORAPP coordinatorApp = mapToCoordinator((Process) process);
        Path path = new Path(StartupProperties.get().get("oozie.workflow.hdfs.path").toString(), process.getName() + ".xml");
        try {
            marshallToHDFS(coordinatorApp, path);
            return super.getWorkflowEngine().schedule(path);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new IvoryException(e);
        }
    }

    @Override
    public String dryRun(Entity process) throws IvoryException {

        COORDINATORAPP coordinatorApp = mapToCoordinator((Process) process);
        Path path = new Path(StartupProperties.get().get("oozie.workflow.hdfs.path").toString(), process.getName() + ".xml");
        try {
            marshallToHDFS(coordinatorApp, path);
            return super.getWorkflowEngine().schedule(path);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new IvoryException(e);
        }
    }

    private COORDINATORAPP mapToCoordinator(Process process) throws IvoryException {
        Map<Entity, EntityType> entityMap = new LinkedHashMap<Entity, EntityType>();

        entityMap.put(process, EntityType.PROCESS);

        for (Input input : process.getInputs().getInput()) {
            Dataset dataset = configStore.get(EntityType.DATASET, input.getFeed());
            assert dataset != null : "No valid dataset found for " + input.getFeed();
            entityMap.put(dataset, EntityType.DATASET);
        }

        for (Output output : process.getOutputs().getOutput()) {
            Dataset dataset = configStore.get(EntityType.DATASET, output.getFeed());
            assert dataset != null : "No valid dataset found for " + output.getFeed();
            entityMap.put(dataset, EntityType.DATASET);
        }

        COORDINATORAPP coordinatorApp = new COORDINATORAPP();
        CoordinatorMapper coordinatorMapper = new CoordinatorMapper(entityMap, coordinatorApp);
        coordinatorMapper.mapToDefaultCoordinator();
        LOG.info("Mapped to default coordinator");
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

    @Override
    public String suspend(Entity process) throws IvoryException {
        return super.getWorkflowEngine().suspend(process.getName());
    }

    @Override
    public String resume(Entity process) throws IvoryException {
        return super.getWorkflowEngine().resume(process.getName());
    }

    @Override
    public String delete(Entity process) throws IvoryException {
        return super.getWorkflowEngine().delete(process.getName());
    }
}
