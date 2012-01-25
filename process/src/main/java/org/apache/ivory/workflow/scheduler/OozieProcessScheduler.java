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

package org.apache.ivory.workflow.scheduler;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
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
import org.apache.ivory.workflow.EntityScheduler;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class OozieProcessScheduler implements EntityScheduler<Process> {

  private static Logger LOG = Logger.getLogger(OozieProcessScheduler.class);

  private static final ConfigurationStore configStore =
      ConfigurationStore.get();

  private final Marshaller marshaller;

  public OozieProcessScheduler() throws Exception {
    JAXBContext jaxbContext = JAXBContext.newInstance(COORDINATORAPP.class);
    marshaller = jaxbContext.createMarshaller();
  }

  @Override
  public String schedule(Process process) throws IvoryException {

    COORDINATORAPP coordinatorApp = mapToCoordinator(process);
    Path path = new Path(StartupProperties.get().
        get("oozie.workflow.hdfs.path").toString(), process.getName() + ".xml");
    try {
      marshallCoordinator(coordinatorApp, path);
      return submitToOozie(path, false);
    } catch (IOException e) {
      throw new IvoryException(e);
    }
  }

  @Override
  public String dryRun(Process process) throws IvoryException {

    COORDINATORAPP coordinatorApp = mapToCoordinator(process);
    Path path = new Path(StartupProperties.get().
        get("oozie.workflow.hdfs.path").toString(), process.getName() + ".xml");
    try {
      marshallCoordinator(coordinatorApp, path);
      return submitToOozie(path, true);
    } catch (IOException e) {
      throw new IvoryException(e);
    }
  }

  private COORDINATORAPP mapToCoordinator(Process process)
      throws IvoryException {
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
    CoordinatorMapper coordinatorMapper = new
        CoordinatorMapper(entityMap, coordinatorApp);
    coordinatorMapper.mapToDefaultCoordinator();
    return coordinatorApp;
  }

  private void marshallCoordinator(COORDINATORAPP coordinatorApp,
                                   Path path) throws IOException, IvoryException {
    ObjectFactory coordinatorObjectFactory = new ObjectFactory();
    JAXBElement<COORDINATORAPP> jaxbCoordinatorApp =
        coordinatorObjectFactory.createCoordinatorApp(coordinatorApp);

    FileSystem fs = path.getFileSystem(new Configuration());
    OutputStream outStream = fs.create(path);
    try {
      marshaller.marshal(jaxbCoordinatorApp, outStream);
    } catch (JAXBException e) {
      throw new IvoryException("Unable to create oozie coordinator app", e);
    } finally {
      outStream.close();
    }
  }

  private String submitToOozie(Path path, boolean dryRun) throws IOException {
    HttpClient client = new HttpClient();
    String oozieUrl = StartupProperties.get().getProperty("oozie.url");
    PostMethod postMethod = new PostMethod(oozieUrl);

    Configuration configuration = new Configuration(false);
    configuration.set("oozie.coord.application.path", path.toString());
    configuration.set("user.name", StartupProperties.get().
            get("user.name").toString());
    configuration.set("nameNode", StartupProperties.get().
            get("nameNode").toString());
    configuration.set("jobTracker", StartupProperties.get().
            get("jobTracker").toString());
    configuration.set("queueName", StartupProperties.get().
            get("queueName").toString());
    ByteArrayOutputStream out = new ByteArrayOutputStream(2048);
    configuration.writeXml(out);
    out.close();
    RequestEntity reqEntity = new ByteArrayRequestEntity
        (out.toByteArray(), "application/xml;charset=UTF-8");
    postMethod.setRequestEntity(reqEntity);
    int status = client.executeMethod(postMethod);
    return postMethod.getResponseBodyAsString();
  }
}
