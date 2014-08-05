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

package org.apache.falcon.regression.Entities;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Clusters;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Properties;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/** Class for representing a process xml. */
public class ProcessMerlin extends Process {
    public ProcessMerlin(String processData) {
        this((Process) fromString(EntityType.PROCESS, processData));
    }

    public ProcessMerlin(final Process process) {
        try {
            PropertyUtils.copyProperties(this, process);
        } catch (IllegalAccessException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (InvocationTargetException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (NoSuchMethodException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * Method sets a number of clusters to process definition.
     *
     * @param newClusters list of definitions of clusters which are to be set to process
     *                    (clusters on which process should run)
     * @param startTime start of process validity on every cluster
     * @param endTime end of process validity on every cluster
     */
    public void setProcessClusters(List<String> newClusters, String startTime, String endTime) {
        Clusters cs =  new Clusters();
        for (String newCluster : newClusters) {
            Cluster c = new Cluster();
            c.setName(new ClusterMerlin(newCluster).getName());
            org.apache.falcon.entity.v0.process.Validity v =
                new org.apache.falcon.entity.v0.process.Validity();
            v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
            v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
            c.setValidity(v);
            cs.getClusters().add(c);
        }
        setClusters(cs);
    }

    public Bundle setFeedsToGenerateData(FileSystem fs, Bundle b) {
        Date start = getClusters().getClusters().get(0).getValidity().getStart();
        Format formatter = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        String startDate = formatter.format(start);
        Date end = getClusters().getClusters().get(0).getValidity().getEnd();
        String endDate = formatter.format(end);

        Map<String, FeedMerlin> inpFeeds = getInputFeeds(b);
        for (FeedMerlin feedElement : inpFeeds.values()) {
            feedElement.getClusters().getClusters().get(0).getValidity()
                .setStart(TimeUtil.oozieDateToDate(startDate).toDate());
            feedElement.getClusters().getClusters().get(0).getValidity()
                .setEnd(TimeUtil.oozieDateToDate(endDate).toDate());
            b.writeFeedElement(feedElement, feedElement.getName());
        }
        return b;
    }

    public Map<String, FeedMerlin> getInputFeeds(Bundle b) {
        Map<String, FeedMerlin> inpFeeds = new HashMap<String, FeedMerlin>();
        for (Input input : getInputs().getInputs()) {
            for (String feed : b.getDataSets()) {
                if (Util.readEntityName(feed).equalsIgnoreCase(input.getFeed())) {
                    FeedMerlin feedO = new FeedMerlin(feed);
                    inpFeeds.put(Util.readEntityName(feed), feedO);
                    break;
                }
            }
        }
        return inpFeeds;
    }

    public final void setProperty(String name, String value) {
        Property p = new Property();
        p.setName(name);
        p.setValue(value);
        if (null == getProperties() || null == getProperties()
            .getProperties() || getProperties().getProperties().size()
            <= 0) {
            Properties props = new Properties();
            props.getProperties().add(p);
            setProperties(props);
        } else {
            getProperties().getProperties().add(p);
        }
    }

    @Override
    public String toString() {
        try {
            StringWriter sw = new StringWriter();
            EntityType.PROCESS.getMarshaller().marshal(this, sw);
            return sw.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    public void renameClusters(Map<String, String> clusterNameMap) {
        for (Cluster cluster : getClusters().getClusters()) {
            final String oldName = cluster.getName();
            final String newName = clusterNameMap.get(oldName);
            if (!StringUtils.isEmpty(newName)) {
                cluster.setName(newName);
            }
        }
    }

    public void renameFeeds(Map<String, String> feedNameMap) {
        for(Input input : getInputs().getInputs()) {
            final String oldName = input.getFeed();
            final String newName = feedNameMap.get(oldName);
            if (!StringUtils.isEmpty(newName)) {
                input.setFeed(newName);
            }
        }
        for(Output output : getOutputs().getOutputs()) {
            final String oldName = output.getFeed();
            final String newName = feedNameMap.get(oldName);
            if (!StringUtils.isEmpty(newName)) {
                output.setFeed(newName);
            }
        }
    }

    /**
     * Sets unique names for the process.
     * @return mapping of old name to new name
     */
    public Map<? extends String, ? extends String> setUniqueName() {
        final String oldName = getName();
        final String newName =  oldName + Util.getUniqueString();
        setName(newName);
        final HashMap<String, String> nameMap = new HashMap<String, String>(1);
        nameMap.put(oldName, newName);
        return nameMap;
    }

    /**
     * Method sets optional/compulsory inputs and outputs of process according to list of feed
     * definitions and matching numeric parameters. Optional inputs are set first and then
     * compulsory ones.
     *
     * @param newDataSets list of feed definitions
     * @param numberOfInputs number of desired inputs
     * @param numberOfOptionalInput how many inputs should be optional
     * @param numberOfOutputs number of outputs
     */
    public void setProcessFeeds(List<String> newDataSets,
                                  int numberOfInputs, int numberOfOptionalInput,
                                  int numberOfOutputs) {
        int numberOfOptionalSet = 0;
        boolean isFirst = true;

        Inputs is = new Inputs();
        for (int i = 0; i < numberOfInputs; i++) {
            Input in = new Input();
            in.setEnd("now(0,0)");
            in.setStart("now(0,-20)");
            if (numberOfOptionalSet < numberOfOptionalInput) {
                in.setOptional(true);
                in.setName("inputData" + i);
                numberOfOptionalSet++;
            } else {
                in.setOptional(false);
                if (isFirst) {
                    in.setName("inputData");
                    isFirst = false;
                } else {
                    in.setName("inputData" + i);
                }
            }
            in.setFeed(new FeedMerlin(newDataSets.get(i)).getName());
            is.getInputs().add(in);
        }

        setInputs(is);
        if (numberOfInputs == 0) {
            setInputs(null);
        }

        Outputs os = new Outputs();
        for (int i = 0; i < numberOfOutputs; i++) {
            Output op = new Output();
            op.setFeed(new FeedMerlin(newDataSets.get(numberOfInputs - i)).getName());
            op.setName("outputData");
            op.setInstance("now(0,0)");
            os.getOutputs().add(op);
        }
        setOutputs(os);
        setLateProcess(null);
    }
}


