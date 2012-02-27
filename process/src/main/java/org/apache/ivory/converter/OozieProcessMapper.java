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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Pair;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.entity.v0.process.Property;
import org.apache.ivory.oozie.coordinator.*;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OozieProcessMapper extends AbstractOozieEntityMapper<Process> {

    private static final String EL_PREFIX = "elext:";
    private static Logger LOG = Logger.getLogger(OozieProcessMapper.class);
    private static final JAXBContext coordJaxbContext;

    static  {
        try {
            coordJaxbContext = JAXBContext.newInstance(COORDINATORAPP.class);
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXB context", e);
        }
    }

    public OozieProcessMapper(Process entity) {
        super(entity);
    }

    @Override
    protected Pair<org.apache.ivory.entity.v0.cluster.Cluster,
            List<COORDINATORAPP>> getCoordinators() throws IvoryException {
        String clusterName = getEntity().getClusters().getCluster().get(0).getName();
        org.apache.ivory.entity.v0.cluster.Cluster cluster = configStore.
                get(EntityType.CLUSTER, clusterName);
        return Pair.of(cluster, Arrays.asList(createDefaultCoordinator()));
    }

    /**
     * Creates default oozie coordinator 
     * 
     * @return COORDINATORAPP
     * @throws IvoryException on Error
     */
    public COORDINATORAPP createDefaultCoordinator() throws IvoryException {
        Process process = getEntity();
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
        String libDir = getLibDirectory(process.getWorkflow().getPath(), clusterName);
        if(libDir != null)
            properties.put(OozieClient.LIBPATH, libDir);

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

        try {
            if (LOG.isDebugEnabled()) {
                Marshaller marshaller = coordJaxbContext.createMarshaller();
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
                StringWriter writer = new StringWriter();
                marshaller.marshal(new ObjectFactory().createCoordinatorApp(coord), writer);
                LOG.debug(writer.getBuffer());
            }
        } catch (JAXBException e) {
            LOG.error("Unable to marshal coordinator app instance for debug", e);
        }

        return coord;
    }

    private String getLibDirectory(String wfpath, String clusterName) throws IvoryException {
        Path path = new Path(wfpath);
        org.apache.ivory.entity.v0.cluster.Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);
        String libDir;
        try {
            FileSystem fs = new Path(ClusterHelper.getHdfsUrl(cluster)).getFileSystem(new Configuration());
            FileStatus status = fs.getFileStatus(path);
            if(status.isDir())
                libDir = path.toString() + "/lib";
            else
                libDir = path.getParent().toString() + "/lib";
            
            if(fs.exists(new Path(libDir)))
                return libDir;
        } catch (IOException e) {
            throw new IvoryException(e);
        }
        return null;
    }

    private SYNCDATASET createDataSet(String feedName, String clusterName) throws IvoryException {
        Feed feed;
        try {
            feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
        if (feed == null) // This should never happen as its checked in process validation
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

}