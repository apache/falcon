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
import org.apache.falcon.regression.core.util.TimeUtil;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/** Class for representing a process xml. */
public class ProcessMerlin extends Process {
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

}


