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
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Sla;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Properties;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.asserts.SoftAssert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/** Class for representing a process xml. */
public class ProcessMerlin extends Process {
    private static final Logger LOGGER = Logger.getLogger(ProcessMerlin.class);
    public ProcessMerlin(String processData) {
        this((Process) TestEntityUtil.fromString(EntityType.PROCESS, processData));
    }

    public ProcessMerlin(final Process process) {
        try {
            PropertyUtils.copyProperties(this, process);
        } catch (ReflectiveOperationException e) {
            Assert.fail("Can't create ProcessMerlin: " + ExceptionUtils.getStackTrace(e));
        }
    }

    public ProcessMerlin clearProcessCluster() {
        getClusters().getClusters().clear();
        return this;
    }

    public ProcessMerlin addProcessCluster(Cluster cluster) {
        getClusters().getClusters().add(cluster);
        return this;
    }

    public List<String> getClusterNames() {
        List<String> names = new ArrayList<>();
        for (Cluster cluster : getClusters().getClusters()) {
            names.add(cluster.getName());
        }
        return names;
    }

    public Cluster getClusterByName(String name) {
        for (Cluster cluster : getClusters().getClusters()) {
            if (name.equals(cluster.getName())) {
                return cluster;
            }
        }
        return null;
    }

    /**
     * Compares two process cluster lists, if they are equal or not.
     */
    public static void assertClustersEqual(List<Cluster> clusters1, List<Cluster> clusters2) {
        if (clusters1.size() != clusters2.size()) {
            Assert.fail("Cluster sizes are different.");
        }
        Comparator<Cluster> clusterComparator = new Comparator<Cluster>() {
            @Override
            public int compare(Cluster cluster1, Cluster cluster2) {
                return cluster1.getName().compareTo(cluster2.getName());
            }
        };
        Collections.sort(clusters1, clusterComparator);
        Collections.sort(clusters2, clusterComparator);
        SoftAssert softAssert = new SoftAssert();
        for(int i = 0; i < clusters1.size(); i++) {
            Cluster cluster1 = clusters1.get(i);
            Cluster cluster2 = clusters2.get(i);
            softAssert.assertEquals(cluster1.getName(), cluster2.getName(), "Cluster names are different.");
            softAssert.assertEquals(cluster1.getValidity().getStart(), cluster2.getValidity().getStart(),
                String.format("Validity start is not the same for cluster %s", cluster1.getName()));
            softAssert.assertEquals(cluster1.getValidity().getEnd(), cluster2.getValidity().getEnd(),
                String.format("Cluster validity end is not the same for cluster %s", cluster1.getName()));
        }
        softAssert.assertAll();
    }

    public Input getInputByName(String name) {
        for (Input input : getInputs().getInputs()) {
            if (input.getName().equals(name)) {
                return input;
            }
        }
        return null;
    }

    public Output getOutputByName(String name) {
        for (Output output : getOutputs().getOutputs()) {
            if (output.getName().equals(name)) {
                return output;
            }
        }
        return null;
    }

    /** Fluent builder wrapper for cluster fragment of process entity . */
    public static class ProcessClusterBuilder {
        private Cluster cluster = new Cluster();

        public ProcessClusterBuilder(String clusterName) {
            cluster.setName(clusterName);
        }

        public Cluster build() {
            Cluster retVal = cluster;
            cluster = null;
            return retVal;
        }

        public ProcessClusterBuilder withValidity(String startTime, String endTime) {
            Validity v = new Validity();
            v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
            v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
            cluster.setValidity(v);
            return this;
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
        clearProcessCluster();
        for (String newCluster : newClusters) {
            final Cluster processCluster = new ProcessClusterBuilder(
                new ClusterMerlin(newCluster).getName())
                .withValidity(startTime, endTime)
                .build();
            addProcessCluster(processCluster);
        }
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
     * @param prefix prefix of new name
     */
    public Map<? extends String, ? extends String> setUniqueName(String prefix) {
        final String oldName = getName();
        final String newName = TestEntityUtil.generateUniqueName(prefix, oldName);
        setName(newName);
        final HashMap<String, String> nameMap = new HashMap<>(1);
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

    /**
     * Sets process pipelines tag.
     * @param pipelines set of pipelines to be set to process
     */
    public void setPipelineTag(String... pipelines){
        if (ArrayUtils.isNotEmpty(pipelines)){
            this.pipelines = StringUtils.join(pipelines, ",");
        } else {
            this.pipelines = null;
        }
    }

    /**
     * Set ACL.
     */
    public void setACL(String owner, String group, String permission) {
        ACL acl = new ACL();
        acl.setOwner(owner);
        acl.setGroup(group);
        acl.setPermission(permission);
        this.setACL(acl);
    }

    /**
     * Set SLA.
     * @param slaStart : start value of SLA
     * @param slaEnd : end value of SLA
     */

    public void setSla(Frequency slaStart, Frequency slaEnd) {
        Sla sla = new Sla();
        sla.setShouldStartIn(slaStart);
        sla.setShouldEndIn(slaEnd);
        this.setSla(sla);
    }

    /**
     * Sets new process validity on all the process clusters.
     *
     * @param startTime start of process validity
     * @param endTime   end of process validity
     */
    public void setValidity(String startTime, String endTime) {

        for (Cluster cluster : this.getClusters().getClusters()) {
            cluster.getValidity().setStart(TimeUtil.oozieDateToDate(startTime).toDate());
            cluster.getValidity().setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
        }
    }

    /**
     * Adds one output into process.
     */
    public void addOutputFeed(String outputName, String feedName) {
        Output out1 = getOutputs().getOutputs().get(0);
        Output out2 = new Output();
        out2.setFeed(feedName);
        out2.setName(outputName);
        out2.setInstance(out1.getInstance());
        getOutputs().getOutputs().add(out2);
    }

    /**
     * Adds one input into process.
     */
    public void addInputFeed(String inputName, String feedName) {
        Input in1 = getInputs().getInputs().get(0);
        Input in2 = new Input();
        in2.setEnd(in1.getEnd());
        in2.setFeed(feedName);
        in2.setName(inputName);
        in2.setPartition(in1.getPartition());
        in2.setStart(in1.getStart());
        in2.setOptional(in1.isOptional());
        getInputs().getInputs().add(in2);
    }

    public void setInputFeedWithEl(String inputFeedName, String startEl, String endEl) {
        Inputs inputs = new Inputs();
        Input input = new Input();
        input.setFeed(inputFeedName);
        input.setStart(startEl);
        input.setEnd(endEl);
        input.setName("inputData");
        inputs.getInputs().add(input);
        this.setInputs(inputs);
    }

    public void setDatasetInstances(String startInstance, String endInstance) {
        this.getInputs().getInputs().get(0).setStart(startInstance);
        this.getInputs().getInputs().get(0).setEnd(endInstance);
    }

    public void setProcessInputStartEnd(String start, String end) {
        for (Input input : this.getInputs().getInputs()) {
            input.setStart(start);
            input.setEnd(end);
        }
    }

    /**
     * Sets name(s) of the process output(s).
     *
     * @param names new names of the outputs
     */
    public void setOutputNames(String... names) {
        Outputs outputs = this.getOutputs();
        Assert.assertEquals(outputs.getOutputs().size(), names.length,
            "Number of output names is not equal to number of outputs in process");
        for (int i = 0; i < names.length; i++) {
            outputs.getOutputs().get(i).setName(names[i]);
        }
        this.setOutputs(outputs);
    }

    /**
     * Sets partition for each input, according to number of supplied partitions.
     *
     * @param partition partitions to be set
     */
    public void setInputPartition(String... partition) {
        for (int i = 0; i < partition.length; i++) {
            this.getInputs().getInputs().get(i).setPartition(partition[i]);
        }
    }

    /**
     * Adds optional property to process definition.
     *
     * @param properties desired properties to be added
     */
    public void addProperties(Property... properties) {
        for (Property property : properties) {
            this.getProperties().getProperties().add(property);
        }
    }

    /**
     * Changes names of process inputs.
     *
     * @param names desired names of inputs
     */
    public void setInputNames(String... names) {
        for (int i = 0; i < names.length; i++) {
            this.getInputs().getInputs().get(i).setName(names[i]);
        }
    }

    public void setPeriodicity(int frequency, Frequency.TimeUnit periodicity) {
        Frequency frq = new Frequency(String.valueOf(frequency), periodicity);
        this.setFrequency(frq);
    }

    public void setTimeOut(int magnitude, Frequency.TimeUnit unit) {
        Frequency frq = new Frequency(String.valueOf(magnitude), unit);
        this.setTimeout(frq);
    }

    public void setWorkflow(String wfPath, String libPath, EngineType engineType) {
        Workflow w = this.getWorkflow();
        if (engineType != null) {
            w.setEngine(engineType);
        }
        if (libPath != null) {
            w.setLib(libPath);
        }
        w.setPath(wfPath);
        this.setWorkflow(w);
    }

    public String getFirstInputName() {
        return getInputs().getInputs().get(0).getName();
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.PROCESS;
    }

    public void assertGeneralProperties(ProcessMerlin newProcess){
        SoftAssert softAssert = new SoftAssert();
        // Assert all the the General Properties
        softAssert.assertEquals(newProcess.getName(), getName(),
            "Process Name is different");
        softAssert.assertEquals(newProcess.getTags(), getTags(),
            "Process Tags Value is different");
        softAssert.assertEquals(newProcess.getWorkflow().getName(), getWorkflow().getName(),
            "Process Workflow Name is different");
        if (getWorkflow().getEngine() == EngineType.OOZIE || getWorkflow().getEngine() == null) {
            softAssert.assertTrue(newProcess.getWorkflow().getEngine() == EngineType.OOZIE
                || newProcess.getWorkflow().getEngine() == null, "Process Workflow Engine is different");
        } else {
            softAssert.assertEquals(newProcess.getWorkflow().getEngine().toString(),
                getWorkflow().getEngine().toString(),
                "Process Workflow Engine is different");
        }
        softAssert.assertEquals(newProcess.getWorkflow().getPath(), getWorkflow().getPath(),
            "Process Workflow Path is different");
        softAssert.assertEquals(newProcess.getACL().getOwner(), getACL().getOwner(),
            "Process ACL Owner is different");
        softAssert.assertEquals(newProcess.getACL().getGroup(), getACL().getGroup(),
            "Process ACL Group is different");
        softAssert.assertEquals(newProcess.getACL().getPermission(), getACL().getPermission(),
            "Process ACL Permission is different");
        softAssert.assertAll();
    }

    public void assertPropertiesInfo(ProcessMerlin newProcess){
        SoftAssert softAssert = new SoftAssert();
        // Assert all the Properties Info
        softAssert.assertEquals(newProcess.getTimezone().getID(), getTimezone().getID(),
            "Process TimeZone is different");
        softAssert.assertEquals(newProcess.getFrequency().getFrequency(), getFrequency().getFrequency(),
            "Process Frequency is different");
        softAssert.assertEquals(newProcess.getFrequency().getTimeUnit().toString(),
            getFrequency().getTimeUnit().toString(),
            "Process Frequency Unit is different");
        softAssert.assertEquals(newProcess.getParallel(), getParallel(),
            "Process Parallel is different");
        softAssert.assertEquals(newProcess.getOrder(), getOrder(),
            "Process Order is different");
        softAssert.assertEquals(newProcess.getRetry().getPolicy().value(),
            getRetry().getPolicy().value(),
            "Process Retry Policy is different");
        softAssert.assertEquals(newProcess.getRetry().getAttempts(),
            getRetry().getAttempts(),
            "Process Retry Attempts is different");
        softAssert.assertEquals(newProcess.getRetry().getDelay().getFrequency(),
            getRetry().getDelay().getFrequency(),
            "Process Delay Frequency is different");
        softAssert.assertEquals(newProcess.getRetry().getDelay().getTimeUnit().name(),
            getRetry().getDelay().getTimeUnit().name(),
            "Process Delay Unit is different");
        softAssert.assertAll();
    }

    /**
     * Asserts equality of process inputs.
     */
    public void assertInputValues(ProcessMerlin newProcess){
        Assert.assertEquals(newProcess.getInputs().getInputs().size(), getInputs().getInputs().size(),
            "Processes have different number of inputs.");
        SoftAssert softAssert = new SoftAssert();
        // Assert all the Input values
        for (int i = 0; i < newProcess.getInputs().getInputs().size(); i++) {
            softAssert.assertEquals(newProcess.getInputs().getInputs().get(i).getName(),
                getInputs().getInputs().get(i).getName(),
                "Process Input Name is different");
            softAssert.assertEquals(newProcess.getInputs().getInputs().get(i).getFeed(),
                getInputs().getInputs().get(i).getFeed(),
                "Process Input Feed is different");
            softAssert.assertEquals(newProcess.getInputs().getInputs().get(i).getStart(),
                getInputs().getInputs().get(i).getStart(),
                "Process Input Start is different");
            softAssert.assertEquals(newProcess.getInputs().getInputs().get(i).getEnd(),
                getInputs().getInputs().get(i).getEnd(),
                "Process Input End is different");
        }
        softAssert.assertAll();
    }

    /**
     * Asserts equality of process outputs.
     */
    public void assertOutputValues(ProcessMerlin newProcess){
        SoftAssert softAssert = new SoftAssert();
        // Assert all the Output values
        softAssert.assertEquals(newProcess.getOutputs().getOutputs().get(0).getName(),
            getOutputs().getOutputs().get(0).getName(),
            "Process Output Name is different");
        softAssert.assertEquals(newProcess.getOutputs().getOutputs().get(0).getFeed(),
            getOutputs().getOutputs().get(0).getFeed(),
            "Process Output Feed is different");
        softAssert.assertEquals(newProcess.getOutputs().getOutputs().get(0).getInstance(),
            getOutputs().getOutputs().get(0).getInstance(),
            "Process Output Instance is different");
        softAssert.assertAll();
    }

    /**
     * Asserts equality of two processes.
     */
    public void assertEquals(ProcessMerlin process) {
        LOGGER.info(String.format("Comparing General Properties: source: %n%s%n and process: %n%n%s",
            Util.prettyPrintXml(toString()), Util.prettyPrintXml(process.toString())));
        assertGeneralProperties(process);
        assertInputValues(process);
        assertOutputValues(process);
        assertPropertiesInfo(process);
        assertClustersEqual(getClusters().getClusters(), process.getClusters().getClusters());
    }

    /**
     * Creates an empty process definition.
     */
    public static ProcessMerlin getEmptyProcess(ProcessMerlin process) {
        ProcessMerlin draft = new ProcessMerlin(process.toString());
        draft.setName("");
        draft.setTags("");
        draft.setACL(null);
        draft.getInputs().getInputs().clear();
        draft.getOutputs().getOutputs().clear();
        draft.setRetry(null);
        draft.clearProcessCluster();
        draft.getProperties().getProperties().clear();
        draft.setFrequency(null);
        draft.setOrder(null);
        draft.setTimezone(null);
        draft.setParallel(0);
        Workflow workflow = new Workflow();
        workflow.setName(null);
        workflow.setPath(null);
        workflow.setVersion(null);
        workflow.setEngine(null);
        draft.setWorkflow(null, null, null);
        return draft;
    }
}


