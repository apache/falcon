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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.entity.v0.process.Property;
import org.apache.ivory.oozie.bundle.BUNDLEAPP;
import org.apache.ivory.oozie.bundle.COORDINATOR;
import org.apache.ivory.oozie.coordinator.ACTION;
import org.apache.ivory.oozie.coordinator.CONFIGURATION;
import org.apache.ivory.oozie.coordinator.CONTROLS;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.DATAIN;
import org.apache.ivory.oozie.coordinator.DATAOUT;
import org.apache.ivory.oozie.coordinator.DATASETS;
import org.apache.ivory.oozie.coordinator.INPUTEVENTS;
import org.apache.ivory.oozie.coordinator.OUTPUTEVENTS;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public class OozieProcessMapper {

    private static final String EL_PREFIX = "elext:";
    private Process process;
    private static Logger LOG = Logger.getLogger(OozieProcessMapper.class);

    public static final String NAME_NODE = "nameNode";
    public static final String JOB_TRACKER = "jobTracker";

    public static final JAXBContext coordJaxbContext;
    public static final JAXBContext bundleJaxbContext;

    static {
        try {
            coordJaxbContext = JAXBContext.newInstance(COORDINATORAPP.class);
            bundleJaxbContext = JAXBContext.newInstance(BUNDLEAPP.class);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    public OozieProcessMapper(Process process) {
        this.process = process;
    }

    /**
     * Creates oozie bundle for the process
     * @param hdfsUrl 
     * 
     * @param workflowPath
     *            - path where coord, bundle defs are stored
     * @return path to bundle
     * @throws IvoryException
     */
    public Path createBundle(String hdfsUrl, Path workflowPath) throws IvoryException {
        BUNDLEAPP bundleApp = new BUNDLEAPP();
        bundleApp.setName(process.getWorkflowName());

        // Add default coordinator
        COORDINATORAPP defCoordApp = createDefaultCoordinator();
        Path defCoordPath = new Path(workflowPath, "defaultCoordinator.xml");
        String coord = marshal(defCoordApp);
        LOG.debug("Default coordinator: " + coord);
        writeToHDFS(coord, hdfsUrl, defCoordPath);
        LOG.debug("Wrote default coordinator to: " + defCoordPath);
        
        COORDINATOR defCoord = new COORDINATOR();
        defCoord.setAppPath("${" + NAME_NODE + "}" + defCoordPath.toString());
        defCoord.setName(defCoordApp.getName());
        defCoord.setConfiguration(createBundleConf());
        bundleApp.getCoordinator().add(defCoord);

        // TODO add coords for late data processing

        Path bundlePath = new Path(workflowPath, "bundle.xml");
        String bundle = marshal(bundleApp);
        LOG.debug("Bundle: " + bundle);
        writeToHDFS(bundle, hdfsUrl, bundlePath);
        LOG.debug("Wrote bundle to: " + bundlePath);
        return bundlePath;
    }

    private org.apache.ivory.oozie.bundle.CONFIGURATION createBundleConf() {
        org.apache.ivory.oozie.bundle.CONFIGURATION conf = new org.apache.ivory.oozie.bundle.CONFIGURATION();
        conf.getProperty().add(createBundleProperty(NAME_NODE, "${" + NAME_NODE + "}"));
        conf.getProperty().add(createBundleProperty(JOB_TRACKER, "${" + JOB_TRACKER + "}"));
        return conf;
    }

    /**
     * Creates default oozie coordinator 
     * 
     * @return COORDINATORAPP
     */
    public COORDINATORAPP createDefaultCoordinator() {
        if (process == null)
            return null;

        COORDINATORAPP coord = new COORDINATORAPP();

        // coord attributes
        coord.setName(process.getWorkflowName() + "_DEFAULT");
        coord.setStart(process.getValidity().getStart());
        coord.setEnd(process.getValidity().getEnd());
        coord.setTimezone(process.getValidity().getTimezone());
        coord.setFrequency("${coord:" + process.getFrequency() + "(" + process.getPeriodicity() + ")}");

        // controls
        CONTROLS controls = new CONTROLS();
        controls.setConcurrency(process.getConcurrency());
        controls.setExecution(process.getExecution());
        coord.setControls(controls);

        // user defined properties
        Map<String, String> properties = new HashMap<String, String>();
        if (process.getProperties() != null) {
            for (Property prop : process.getProperties().getProperty())
                properties.put(prop.getName(), prop.getValue());
        }

        String clusterName = process.getClusters().getCluster().get(0).getName();
        // inputs
        if (process.getInputs() != null) {
            for (Input input : process.getInputs().getInput()) {
                SYNCDATASET syncdataset = createDataSet(input.getFeed(), clusterName);
                if (coord.getDatasets() == null)
                    coord.setDatasets(new DATASETS());
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAIN datain = new DATAIN();
                datain.setName(input.getName());
                datain.setDataset(input.getFeed());
                datain.setStartInstance(getELExpression(input.getStartInstance()));
                datain.setEndInstance(getELExpression(input.getEndInstance()));
                if (coord.getInputEvents() == null)
                    coord.setInputEvents(new INPUTEVENTS());
                coord.getInputEvents().getDataIn().add(datain);

                if(StringUtils.isNotEmpty(input.getPartition()))
                    properties.put(input.getName(), getELExpression("dataIn('" + input.getName() + "', '" + input.getPartition() + "')"));
                else
                    properties.put(input.getName(), "${coord:dataIn('" + input.getName() + "')}");                    
            }
        }

        // outputs
        if (process.getOutputs() != null) {
            for (Output output : process.getOutputs().getOutput()) {
                SYNCDATASET syncdataset = createDataSet(output.getFeed(), clusterName);
                if (coord.getDatasets() == null)
                    coord.setDatasets(new DATASETS());
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAOUT dataout = new DATAOUT();
                dataout.setName(output.getName());
                dataout.setDataset(output.getFeed());
                dataout.setInstance(getELExpression(output.getInstance()));
                if (coord.getOutputEvents() == null)
                    coord.setOutputEvents(new OUTPUTEVENTS());
                coord.getOutputEvents().getDataOut().add(dataout);

                properties.put(output.getName(), "${coord:dataOut('" + output.getName() + "')}");
            }
        }

        // add default properties
        properties.put(NAME_NODE, "${" + NAME_NODE + "}");
        properties.put(JOB_TRACKER, "${" + JOB_TRACKER + "}");
        properties.put(OozieClient.LIBPATH, process.getWorkflow().getPath() + "/lib");

        //configuration
        CONFIGURATION conf = new CONFIGURATION();
        for(Entry<String, String> entry:properties.entrySet())
            conf.getProperty().add(createCoordProperty(entry.getKey(), entry.getValue()));
            
        //action
        WORKFLOW wf = new WORKFLOW();
        wf.setAppPath(process.getWorkflow().getPath());
        wf.setConfiguration(conf);

        ACTION action = new ACTION();
        action.setWorkflow(wf);
        coord.setAction(action);

        return coord;
    }

    private org.apache.ivory.oozie.coordinator.CONFIGURATION.Property createCoordProperty(String name, String value) {
        org.apache.ivory.oozie.coordinator.CONFIGURATION.Property prop = new org.apache.ivory.oozie.coordinator.CONFIGURATION.Property();
        prop.setName(name);
        prop.setValue(value);
        return prop;
    }

    private org.apache.ivory.oozie.bundle.CONFIGURATION.Property createBundleProperty(String name, String value) {
        org.apache.ivory.oozie.bundle.CONFIGURATION.Property prop = new org.apache.ivory.oozie.bundle.CONFIGURATION.Property();
        prop.setName(name);
        prop.setValue(value);
        return prop;
    }

    private SYNCDATASET createDataSet(String feedName, String clusterName) {
        Feed feed;
        try {
            feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (feed == null) // This should never happen as its checked in process
                          // validation
            throw new RuntimeException("Referenced feed " + feedName + " is not registered!");

        SYNCDATASET syncdataset = new SYNCDATASET();
        syncdataset.setName(feed.getName());
        syncdataset.setUriTemplate("${nameNode}" + feed.getLocations().get(LocationType.DATA).getPath());
        syncdataset.setFrequency("${coord:" + feed.getFrequency() + "(" + feed.getPeriodicity() + ")}");

        Cluster cluster = getCluster(feed.getClusters().getCluster(), clusterName);
        syncdataset.setInitialInstance(cluster.getValidity().getStart());
        syncdataset.setTimezone(cluster.getValidity().getTimezone());
        syncdataset.setDoneFlag("");
        return syncdataset;
    }

    private Cluster getCluster(List<Cluster> clusters, String clusterName) {
        if (clusters != null) {
            for (Cluster cluster : clusters)
                if (cluster.getName().equals(clusterName))
                    return cluster;
        }
        return null;
    }

    private String getELExpression(String expr) {
        if (expr != null) {
            expr = "${" + EL_PREFIX + expr + "}";
        }
        return expr;
    }

    private String marshal(JAXBElement<?> jaxbElement, JAXBContext jaxbContext) throws IvoryException {
        try{
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            StringWriter writer = new StringWriter();
            marshaller.marshal(jaxbElement, writer);
            return writer.toString();
        }catch(Exception e) {
            throw new IvoryException(e);
        }
    }
    
    private void writeToHDFS(String str, String hdfsUrl, Path path) throws IvoryException {
        OutputStreamWriter writer = null;
        try{
            FileSystem fs = new Path(hdfsUrl).getFileSystem(new Configuration());
            writer = new OutputStreamWriter(fs.create(path));
            writer.write(str);
        } catch(Exception e) {
            throw new IvoryException(e);
        } finally {
            if(writer != null)
                try {
                    writer.close();
                } catch (IOException e) {
                    throw new IvoryException(e);
                }
        }
    }
    
    private String marshal(COORDINATORAPP coord) throws IvoryException {
        return marshal(new ObjectFactory().createCoordinatorApp(coord), coordJaxbContext);
    }

    private String marshal(BUNDLEAPP bundle) throws IvoryException {
        return marshal(new org.apache.ivory.oozie.bundle.ObjectFactory().createBundleApp(bundle), bundleJaxbContext);
    }
}