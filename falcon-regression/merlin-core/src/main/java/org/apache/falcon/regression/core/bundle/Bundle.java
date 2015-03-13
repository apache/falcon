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

package org.apache.falcon.regression.core.bundle;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.LateProcess;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A bundle abstraction.
 */
public class Bundle {

    private static final String PRISM_PREFIX = "prism";
    private static ColoHelper prismHelper = new ColoHelper(PRISM_PREFIX);
    private static final Logger LOGGER = Logger.getLogger(Bundle.class);

    private List<String> clusters;
    private List<String> dataSets;
    private String processData;

    public void submitFeed()
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        InterruptedException {
        submitClusters(prismHelper);

        AssertUtil.assertSucceeded(prismHelper.getFeedHelper().submitEntity(dataSets.get(0)));
    }

    public void submitAndScheduleFeed() throws Exception {
        submitClusters(prismHelper);

        AssertUtil.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(dataSets.get(0)));
    }

    public void submitAndScheduleFeedUsingColoHelper(ColoHelper coloHelper) throws Exception {
        submitFeed();

        AssertUtil.assertSucceeded(coloHelper.getFeedHelper().schedule(dataSets.get(0)));
    }

    public void submitAndScheduleAllFeeds()
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        submitClusters(prismHelper);

        for (String feed : dataSets) {
            AssertUtil.assertSucceeded(prismHelper.getFeedHelper().submitAndSchedule(feed));
        }
    }

    public ServiceResponse submitProcess(boolean shouldSucceed) throws JAXBException,
        IOException, URISyntaxException, AuthenticationException, InterruptedException {
        submitClusters(prismHelper);
        submitFeeds(prismHelper);
        ServiceResponse r = prismHelper.getProcessHelper().submitEntity(processData);
        if (shouldSucceed) {
            AssertUtil.assertSucceeded(r);
        } else {
            AssertUtil.assertFailed(r);
        }
        return r;
    }

    public void submitFeedsScheduleProcess() throws Exception {
        submitClusters(prismHelper);

        submitFeeds(prismHelper);

        AssertUtil.assertSucceeded(prismHelper.getProcessHelper().submitAndSchedule(processData));
    }


    public void submitAndScheduleProcess() throws Exception {
        submitAndScheduleAllFeeds();

        AssertUtil.assertSucceeded(prismHelper.getProcessHelper().submitAndSchedule(processData));
    }

    public void submitAndScheduleProcessUsingColoHelper(ColoHelper coloHelper) throws Exception {
        submitProcess(true);

        AssertUtil.assertSucceeded(coloHelper.getProcessHelper().schedule(processData));
    }

    public List<String> getClusters() {
        return clusters;
    }

    public Bundle(String clusterData, List<String> dataSets, String processData) {
        this.dataSets = dataSets;
        this.processData = processData;
        this.clusters = new ArrayList<String>();
        this.clusters.add(clusterData);
    }

    public Bundle(Bundle bundle, String prefix) {
        this.dataSets = new ArrayList<String>(bundle.getDataSets());
        this.processData = bundle.getProcessData();
        this.clusters = new ArrayList<String>();
        for (String cluster : bundle.getClusters()) {
            this.clusters.add(Util.getEnvClusterXML(cluster, prefix));
        }
    }

    public Bundle(Bundle bundle, ColoHelper helper) {
        this(bundle, helper.getPrefix());
    }

    public void setClusterData(List<String> pClusters) {
        this.clusters = new ArrayList<String>(pClusters);
    }
    /**
     * Unwraps cluster element to string and writes it to bundle.
     *
     * @param c      Cluster object to be unwrapped and set into bundle
     */
    public void writeClusterElement(org.apache.falcon.entity.v0.cluster.Cluster c) {
        final List<String> newClusters = new ArrayList<String>();
        newClusters.add(c.toString());
        setClusterData(newClusters);
    }

    /**
     * Wraps bundle cluster in a Cluster object.
     *
     * @return cluster definition in a form of Cluster object
     */
    public ClusterMerlin getClusterElement() {
        return new ClusterMerlin(getClusters().get(0));
    }


    public List<String> getClusterNames() {
        List<String> clusterNames = new ArrayList<String>();
        for (String cluster : clusters) {
            final org.apache.falcon.entity.v0.cluster.Cluster clusterObject =
                Util.getClusterObject(cluster);
            clusterNames.add(clusterObject.getName());
        }
        return clusterNames;
    }

    public List<String> getDataSets() {
        return dataSets;
    }

    public void setDataSets(List<String> dataSets) {
        this.dataSets = dataSets;
    }

    public String getProcessData() {
        return processData;
    }

    public void setProcessData(String processData) {
        this.processData = processData;
    }

    /**
     * Generates unique entities within a bundle changing their names and names of dependant items
     * to unique.
     */
    public void generateUniqueBundle(Object testClassObject) {
        generateUniqueBundle(testClassObject.getClass().getSimpleName() + '-');
    }

    /**
     * Generates unique entities within a bundle changing their names and names of dependant items
     * to unique.
     */
    public void generateUniqueBundle(String prefix) {
        /* creating new names */
        List<ClusterMerlin> clusterMerlinList = ClusterMerlin.fromString(clusters);
        Map<String, String> clusterNameMap = new HashMap<String, String>();
        for (ClusterMerlin clusterMerlin : clusterMerlinList) {
            clusterNameMap.putAll(clusterMerlin.setUniqueName(prefix));
        }

        List<FeedMerlin> feedMerlinList = FeedMerlin.fromString(dataSets);
        Map<String, String> feedNameMap = new HashMap<String, String>();
        for (FeedMerlin feedMerlin : feedMerlinList) {
            feedNameMap.putAll(feedMerlin.setUniqueName(prefix));
        }

        /* setting new names in feeds and process */
        for (FeedMerlin feedMerlin : feedMerlinList) {
            feedMerlin.renameClusters(clusterNameMap);
        }

        /* setting variables */
        clusters.clear();
        for (ClusterMerlin clusterMerlin : clusterMerlinList) {
            clusters.add(clusterMerlin.toString());
        }
        dataSets.clear();
        for (FeedMerlin feedMerlin : feedMerlinList) {
            dataSets.add(feedMerlin.toString());
        }

        if (StringUtils.isNotEmpty(processData)) {
            ProcessMerlin processMerlin = new ProcessMerlin(processData);
            processMerlin.setUniqueName(prefix);
            processMerlin.renameClusters(clusterNameMap);
            processMerlin.renameFeeds(feedNameMap);
            processData = processMerlin.toString();
        }
    }

    public ServiceResponse submitBundle(ColoHelper helper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {

        submitClusters(helper);

        //lets submit all data first
        submitFeeds(helper);

        return helper.getProcessHelper().submitEntity(getProcessData());
    }

    /**
     * Submit all the entities and schedule the process.
     *
     * @param helper helper of prism host
     * @return message from schedule response
     * @throws IOException
     * @throws JAXBException
     * @throws URISyntaxException
     * @throws AuthenticationException
     */
    public String submitFeedsScheduleProcess(ColoHelper helper)
        throws IOException, JAXBException, URISyntaxException,
        AuthenticationException, InterruptedException {
        ServiceResponse submitResponse = submitBundle(helper);
        if (submitResponse.getCode() == 400) {
            return submitResponse.getMessage();
        }

        //lets schedule the damn thing now :)
        ServiceResponse scheduleResult = helper.getProcessHelper().schedule(getProcessData());
        AssertUtil.assertSucceeded(scheduleResult);
        TimeUtil.sleepSeconds(7);
        return scheduleResult.getMessage();
    }

    /**
     * Sets the only process input.
     *
     * @param startEl its start in terms of EL expression
     * @param endEl its end in terms of EL expression
     */
    public void setProcessInput(String startEl, String endEl) {
        ProcessMerlin process = getProcessObject();
        Inputs inputs = new Inputs();
        Input input = new Input();
        input.setFeed(Util.readEntityName(getInputFeedFromBundle()));
        input.setStart(startEl);
        input.setEnd(endEl);
        input.setName("inputData");
        inputs.getInputs().add(input);
        process.setInputs(inputs);
        this.setProcessData(process.toString());
    }

    public void setInvalidData() {
        int index = 0;
        FeedMerlin dataElement = new FeedMerlin(dataSets.get(0));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = new FeedMerlin(dataSets.get(1));
            index = 1;
        }


        String oldLocation = dataElement.getLocations().getLocations().get(0).getPath();
        LOGGER.info("oldlocation: " + oldLocation);
        dataElement.getLocations().getLocations().get(0).setPath(
            oldLocation.substring(0, oldLocation.indexOf('$')) + "invalid/"
                    +
                oldLocation.substring(oldLocation.indexOf('$')));
        LOGGER.info("new location: " + dataElement.getLocations().getLocations().get(0).getPath());
        dataSets.set(index, dataElement.toString());
    }


    public void setFeedValidity(String feedStart, String feedEnd, String feedName) {
        FeedMerlin feedElement = getFeedElement(feedName);
        feedElement.getClusters().getClusters().get(0).getValidity()
            .setStart(TimeUtil.oozieDateToDate(feedStart).toDate());
        feedElement.getClusters().getClusters().get(0).getValidity()
            .setEnd(TimeUtil.oozieDateToDate(feedEnd).toDate());
        writeFeedElement(feedElement, feedName);
    }

    public int getInitialDatasetFrequency() {
        FeedMerlin dataElement = new FeedMerlin(dataSets.get(0));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = new FeedMerlin(dataSets.get(1));
        }
        if (dataElement.getFrequency().getTimeUnit() == TimeUnit.hours) {
            return (Integer.parseInt(dataElement.getFrequency().getFrequency())) * 60;
        } else {
            return (Integer.parseInt(dataElement.getFrequency().getFrequency()));
        }
    }

    public Date getStartInstanceProcess(Calendar time) {
        ProcessMerlin processElement = getProcessObject();
        LOGGER.info("start instance: " + processElement.getInputs().getInputs().get(0).getStart());
        return TimeUtil.getMinutes(processElement.getInputs().getInputs().get(0).getStart(), time);
    }

    public Date getEndInstanceProcess(Calendar time) {
        ProcessMerlin processElement = getProcessObject();
        LOGGER.info("end instance: " + processElement.getInputs().getInputs().get(0).getEnd());
        LOGGER.info("timezone in getendinstance: " + time.getTimeZone().toString());
        LOGGER.info("time in getendinstance: " + time.getTime());
        return TimeUtil.getMinutes(processElement.getInputs().getInputs().get(0).getEnd(), time);
    }

    public void setDatasetInstances(String startInstance, String endInstance) {
        ProcessMerlin processElement = getProcessObject();
        processElement.getInputs().getInputs().get(0).setStart(startInstance);
        processElement.getInputs().getInputs().get(0).setEnd(endInstance);
        setProcessData(processElement.toString());
    }

    public void setProcessPeriodicity(int frequency, TimeUnit periodicity) {
        ProcessMerlin processElement = getProcessObject();
        Frequency frq = new Frequency("" + frequency, periodicity);
        processElement.setFrequency(frq);
        setProcessData(processElement.toString());
    }

    public void setProcessInputStartEnd(String start, String end) {
        ProcessMerlin processElement = getProcessObject();
        for (Input input : processElement.getInputs().getInputs()) {
            input.setStart(start);
            input.setEnd(end);
        }
        setProcessData(processElement.toString());
    }

    public void setOutputFeedPeriodicity(int frequency, TimeUnit periodicity) {
        ProcessMerlin processElement = new ProcessMerlin(processData);
        String outputDataset = null;
        int datasetIndex;
        for (datasetIndex = 0; datasetIndex < dataSets.size(); datasetIndex++) {
            outputDataset = dataSets.get(datasetIndex);
            if (outputDataset.contains(processElement.getOutputs().getOutputs().get(0).getFeed())) {
                break;
            }
        }

        FeedMerlin feedElement = new FeedMerlin(outputDataset);

        feedElement.setFrequency(new Frequency("" + frequency, periodicity));
        dataSets.set(datasetIndex, feedElement.toString());
        LOGGER.info("modified o/p dataSet is: " + dataSets.get(datasetIndex));
    }

    public int getProcessConcurrency() {
        return getProcessObject().getParallel();
    }

    public void setOutputFeedLocationData(String path) {
        ProcessMerlin processElement = new ProcessMerlin(processData);
        String outputDataset = null;
        int datasetIndex;
        for (datasetIndex = 0; datasetIndex < dataSets.size(); datasetIndex++) {
            outputDataset = dataSets.get(datasetIndex);
            if (outputDataset.contains(processElement.getOutputs().getOutputs().get(0).getFeed())) {
                break;
            }
        }

        FeedMerlin feedElement = new FeedMerlin(outputDataset);
        Location l = new Location();
        l.setPath(path);
        l.setType(LocationType.DATA);
        feedElement.getLocations().getLocations().set(0, l);
        dataSets.set(datasetIndex, feedElement.toString());
        LOGGER.info("modified location path dataSet is: " + dataSets.get(datasetIndex));
    }

    public void setProcessConcurrency(int concurrency) {
        ProcessMerlin processElement = getProcessObject();
        processElement.setParallel((concurrency));
        setProcessData(processElement.toString());
    }

    public void setProcessWorkflow(String wfPath) {
        setProcessWorkflow(wfPath, null);
    }

    public void setProcessWorkflow(String wfPath, EngineType engineType) {
        setProcessWorkflow(wfPath, null, engineType);
    }

    public void setProcessWorkflow(String wfPath, String libPath, EngineType engineType) {
        ProcessMerlin processElement = getProcessObject();
        Workflow w = processElement.getWorkflow();
        if (engineType != null) {
            w.setEngine(engineType);
        }
        if (libPath != null) {
            w.setLib(libPath);
        }
        w.setPath(wfPath);
        processElement.setWorkflow(w);
        setProcessData(processElement.toString());
    }

    public ProcessMerlin getProcessObject() {
        return new ProcessMerlin(getProcessData());
    }

    public FeedMerlin getFeedElement(String feedName) {
        return new FeedMerlin(getFeed(feedName));
    }


    public String getFeed(String feedName) {
        for (String feed : getDataSets()) {
            if (Util.readEntityName(feed).contains(feedName)) {
                return feed;
            }
        }

        return null;
    }


    public void writeFeedElement(FeedMerlin feedElement, String feedName) {
        writeFeedElement(feedElement.toString(), feedName);
    }


    public void writeFeedElement(String feedString, String feedName) {
        dataSets.set(dataSets.indexOf(getFeed(feedName)), feedString);
    }


    public void setInputFeedPeriodicity(int frequency, TimeUnit periodicity) {
        String feedName = getInputFeedNameFromBundle();
        FeedMerlin feedElement = getFeedElement(feedName);
        Frequency frq = new Frequency("" + frequency, periodicity);
        feedElement.setFrequency(frq);
        writeFeedElement(feedElement, feedName);

    }

    public void setInputFeedValidity(String startInstance, String endInstance) {
        String feedName = getInputFeedNameFromBundle();
        this.setFeedValidity(startInstance, endInstance, feedName);
    }

    public void setOutputFeedValidity(String startInstance, String endInstance) {
        String feedName = getOutputFeedNameFromBundle();
        this.setFeedValidity(startInstance, endInstance, feedName);
    }

    public void setInputFeedDataPath(String path) {
        String feedName = getInputFeedNameFromBundle();
        FeedMerlin feedElement = getFeedElement(feedName);
        final List<Location> locations = feedElement.getLocations().getLocations();
        for (Location location : locations) {
            if (location.getType() == LocationType.DATA) {
                locations.get(0).setPath(path);
            }
        }
        writeFeedElement(feedElement, feedName);
    }

    public String getFeedDataPathPrefix() {
        FeedMerlin feedElement =
            getFeedElement(getInputFeedNameFromBundle());
        return Util.getPathPrefix(feedElement.getLocations().getLocations().get(0)
            .getPath());
    }

    public void setProcessValidity(DateTime startDate, DateTime endDate) {

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/HH:mm");

        String start = formatter.print(startDate).replace("/", "T") + "Z";
        String end = formatter.print(endDate).replace("/", "T") + "Z";

        ProcessMerlin processElement = new ProcessMerlin(processData);

        for (Cluster cluster : processElement.getClusters().getClusters()) {

            org.apache.falcon.entity.v0.process.Validity validity =
                new org.apache.falcon.entity.v0.process.Validity();
            validity.setStart(TimeUtil.oozieDateToDate(start).toDate());
            validity.setEnd(TimeUtil.oozieDateToDate(end).toDate());
            cluster.setValidity(validity);

        }

        processData = processElement.toString();
    }

    public void setProcessValidity(String startDate, String endDate) {
        ProcessMerlin processElement = new ProcessMerlin(processData);

        for (Cluster cluster : processElement.getClusters().getClusters()) {
            org.apache.falcon.entity.v0.process.Validity validity =
                new org.apache.falcon.entity.v0.process.Validity();
            validity.setStart(TimeUtil.oozieDateToDate(startDate).toDate());
            validity.setEnd(TimeUtil.oozieDateToDate(endDate).toDate());
            cluster.setValidity(validity);

        }

        processData = processElement.toString();
    }

    public void setProcessLatePolicy(LateProcess lateProcess) {
        ProcessMerlin processElement = new ProcessMerlin(processData);
        processElement.setLateProcess(lateProcess);
        processData = processElement.toString();
    }


    public void verifyDependencyListing(ColoHelper coloHelper)
        throws InterruptedException, IOException, AuthenticationException, URISyntaxException {
        //display dependencies of process:
        String dependencies = coloHelper.getProcessHelper().getDependencies(
            Util.readEntityName(getProcessData())).getEntityList().toString();

        //verify presence
        for (String cluster : clusters) {
            Assert.assertTrue(dependencies.contains("(cluster) " + Util.readEntityName(cluster)));
        }
        for (String feed : getDataSets()) {
            Assert.assertTrue(dependencies.contains("(feed) " + Util.readEntityName(feed)));
            for (String cluster : clusters) {
                Assert.assertTrue(coloHelper.getFeedHelper().getDependencies(
                    Util.readEntityName(feed)).getEntityList().toString()
                    .contains("(cluster) " + Util.readEntityName(cluster)));
            }
            Assert.assertFalse(coloHelper.getFeedHelper().getDependencies(
                Util.readEntityName(feed)).getEntityList().toString()
                .contains("(process)" + Util.readEntityName(getProcessData())));
        }
    }

    public void addProcessInput(String feed, String feedName) {
        ProcessMerlin processElement = getProcessObject();
        Input in1 = processElement.getInputs().getInputs().get(0);
        Input in2 = new Input();
        in2.setEnd(in1.getEnd());
        in2.setFeed(feed);
        in2.setName(feedName);
        in2.setPartition(in1.getPartition());
        in2.setStart(in1.getStart());
        processElement.getInputs().getInputs().add(in2);
        setProcessData(processElement.toString());
    }

    public void setProcessName(String newName) {
        ProcessMerlin processElement = getProcessObject();
        processElement.setName(newName);
        setProcessData(processElement.toString());

    }

    public void setRetry(Retry retry) {
        LOGGER.info("old process: " + Util.prettyPrintXml(processData));
        ProcessMerlin processObject = getProcessObject();
        processObject.setRetry(retry);
        processData = processObject.toString();
        LOGGER.info("updated process: " + Util.prettyPrintXml(processData));
    }

    public void setInputFeedAvailabilityFlag(String flag) {
        String feedName = getInputFeedNameFromBundle();
        FeedMerlin feedElement = getFeedElement(feedName);
        feedElement.setAvailabilityFlag(flag);
        writeFeedElement(feedElement, feedName);
    }

    public void setOutputFeedAvailabilityFlag(String flag) {
        String feedName = getOutputFeedNameFromBundle();
        FeedMerlin feedElement = getFeedElement(feedName);
        feedElement.setAvailabilityFlag(flag);
        writeFeedElement(feedElement, feedName);
    }

    public void setCLusterColo(String colo) {
        ClusterMerlin c = getClusterElement();
        c.setColo(colo);
        writeClusterElement(c);

    }

    public void setClusterInterface(Interfacetype interfacetype, String value) {
        ClusterMerlin c = getClusterElement();
        final Interfaces interfaces = c.getInterfaces();
        final List<Interface> interfaceList = interfaces.getInterfaces();
        for (final Interface anInterface : interfaceList) {
            if (anInterface.getType() == interfacetype) {
                anInterface.setEndpoint(value);
            }
        }
        writeClusterElement(c);
    }

    public void setInputFeedTableUri(String tableUri) {
        final String feedStr = getInputFeedFromBundle();
        FeedMerlin feed = new FeedMerlin(feedStr);
        final CatalogTable catalogTable = new CatalogTable();
        catalogTable.setUri(tableUri);
        feed.setTable(catalogTable);
        writeFeedElement(feed, feed.getName());
    }

    public void setOutputFeedTableUri(String tableUri) {
        final String feedStr = getOutputFeedFromBundle();
        FeedMerlin feed = new FeedMerlin(feedStr);
        final CatalogTable catalogTable = new CatalogTable();
        catalogTable.setUri(tableUri);
        feed.setTable(catalogTable);
        writeFeedElement(feed, feed.getName());
    }

    public void setCLusterWorkingPath(String clusterData, String path) {
        ClusterMerlin c = new ClusterMerlin(clusterData);
        for (int i = 0; i < c.getLocations().getLocations().size(); i++) {
            if (c.getLocations().getLocations().get(i).getName().equals(ClusterLocationType.WORKING)) {
                c.getLocations().getLocations().get(i).setPath(path);
            }
        }

        //this.setClusterData(clusterData)
        writeClusterElement(c);
    }


    public void submitClusters(ColoHelper helper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        submitClusters(helper, null);
    }

    public void submitClusters(ColoHelper helper, String user)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        for (String cluster : this.clusters) {
            AssertUtil.assertSucceeded(helper.getClusterHelper().submitEntity(cluster, user));
        }
    }

    public void submitFeeds(ColoHelper helper)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        for (String feed : this.dataSets) {
            AssertUtil.assertSucceeded(helper.getFeedHelper().submitEntity(feed));
        }
    }

    public void addClusterToBundle(String clusterData, ClusterType type,
                                   String startTime, String endTime) {
        clusterData = setNewClusterName(clusterData);

        this.clusters.add(clusterData);
        //now to add clusters to feeds
        for (int i = 0; i < dataSets.size(); i++) {
            FeedMerlin feedObject = new FeedMerlin(dataSets.get(i));
            org.apache.falcon.entity.v0.feed.Cluster cluster =
                new org.apache.falcon.entity.v0.feed.Cluster();
            cluster.setName(Util.getClusterObject(clusterData).getName());
            cluster.setValidity(feedObject.getClusters().getClusters().get(0).getValidity());
            cluster.setType(type);
            cluster.setRetention(feedObject.getClusters().getClusters().get(0).getRetention());
            feedObject.getClusters().getClusters().add(cluster);

            dataSets.remove(i);
            dataSets.add(i, feedObject.toString());

        }

        //now to add cluster to process
        ProcessMerlin processObject = new ProcessMerlin(processData);
        Cluster cluster = new Cluster();
        cluster.setName(Util.getClusterObject(clusterData).getName());
        org.apache.falcon.entity.v0.process.Validity v =
            processObject.getClusters().getClusters().get(0).getValidity();
        if (StringUtils.isNotEmpty(startTime)) {
            v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
        }
        if (StringUtils.isNotEmpty(endTime)) {
            v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
        }
        cluster.setValidity(v);
        processObject.getClusters().getClusters().add(cluster);
        this.processData = processObject.toString();

    }

    private String setNewClusterName(String clusterData) {
        ClusterMerlin clusterObj = new ClusterMerlin(clusterData);
        clusterObj.setName(clusterObj.getName() + this.clusters.size() + 1);
        return clusterObj.toString();
    }

    public void deleteBundle(ColoHelper helper) {

        try {
            helper.getProcessHelper().delete(getProcessData());
        } catch (Exception e) {
            e.getStackTrace();
        }

        for (String dataset : getDataSets()) {
            try {
                helper.getFeedHelper().delete(dataset);
            } catch (Exception e) {
                e.getStackTrace();
            }
        }

        for (String cluster : this.getClusters()) {
            try {
                helper.getClusterHelper().delete(cluster);
            } catch (Exception e) {
                e.getStackTrace();
            }
        }


    }

    public String getProcessName() {

        return Util.getProcessName(this.getProcessData());
    }

    public void setProcessLibPath(String libPath) {
        ProcessMerlin processElement = getProcessObject();
        Workflow wf = processElement.getWorkflow();
        wf.setLib(libPath);
        processElement.setWorkflow(wf);
        setProcessData(processElement.toString());

    }

    public void setProcessTimeOut(int magnitude, TimeUnit unit) {
        ProcessMerlin processElement = getProcessObject();
        Frequency frq = new Frequency("" + magnitude, unit);
        processElement.setTimeout(frq);
        setProcessData(processElement.toString());
    }

    public static void submitCluster(Bundle... bundles)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {

        for (Bundle bundle : bundles) {
            ServiceResponse r =
                prismHelper.getClusterHelper().submitEntity(bundle.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());
        }


    }

    /**
     * Generates unique entities definitions: clusters, feeds and process, populating them with
     * desired values of different properties.
     *
     * @param numberOfClusters number of clusters on which feeds and process should run
     * @param numberOfInputs number of desired inputs in process definition
     * @param numberOfOptionalInput how many inputs should be optional
     * @param inputBasePaths base data path for inputs
     * @param numberOfOutputs number of outputs
     * @param startTime start of feeds and process validity on every cluster
     * @param endTime end of feeds and process validity on every cluster
     */
    public void generateRequiredBundle(int numberOfClusters, int numberOfInputs,
                                       int numberOfOptionalInput,
                                       String inputBasePaths, int numberOfOutputs, String startTime,
                                       String endTime) {
        //generate and set clusters
        ClusterMerlin c = new ClusterMerlin(getClusters().get(0));
        List<String> newClusters = new ArrayList<String>();
        final String clusterName = c.getName();
        for (int i = 0; i < numberOfClusters; i++) {
            c.setName(clusterName + i);
            newClusters.add(i, c.toString());
        }
        setClusterData(newClusters);

        //generate and set newDataSets
        List<String> newDataSets = new ArrayList<String>();
        for (int i = 0; i < numberOfInputs; i++) {
            final FeedMerlin feed = new FeedMerlin(getDataSets().get(0));
            feed.setName(feed.getName() + "-input" + i);
            feed.setFeedClusters(newClusters, inputBasePaths + "/input" + i, startTime, endTime);
            newDataSets.add(feed.toString());
        }
        for (int i = 0; i < numberOfOutputs; i++) {
            final FeedMerlin feed = new FeedMerlin(getDataSets().get(0));
            feed.setName(feed.getName() + "-output" + i);
            feed.setFeedClusters(newClusters, inputBasePaths + "/output" + i,  startTime, endTime);
            newDataSets.add(feed.toString());
        }
        setDataSets(newDataSets);

        //add clusters and feed to process
        ProcessMerlin processMerlin = new ProcessMerlin(getProcessData());
        processMerlin.setProcessClusters(newClusters, startTime, endTime);
        processMerlin.setProcessFeeds(newDataSets, numberOfInputs,
            numberOfOptionalInput, numberOfOutputs);
        setProcessData(processMerlin.toString());
    }

    public void submitAndScheduleBundle(ColoHelper helper, boolean checkSuccess)
        throws IOException, JAXBException, URISyntaxException, AuthenticationException,
            InterruptedException {

        for (int i = 0; i < getClusters().size(); i++) {
            ServiceResponse r;
            r = helper.getClusterHelper().submitEntity(getClusters().get(i));
            if (checkSuccess) {
                AssertUtil.assertSucceeded(r);
            }
        }
        for (int i = 0; i < getDataSets().size(); i++) {
            ServiceResponse r = helper.getFeedHelper().submitAndSchedule(getDataSets().get(i));
            if (checkSuccess) {
                AssertUtil.assertSucceeded(r);
            }
        }
        ServiceResponse r = helper.getProcessHelper().submitAndSchedule(getProcessData());
        if (checkSuccess) {
            AssertUtil.assertSucceeded(r);
        }
    }

    /**
     * Changes names of process inputs.
     *
     * @param names desired names of inputs
     */
    public void setProcessInputNames(String... names) {
        ProcessMerlin p = new ProcessMerlin(processData);
        for (int i = 0; i < names.length; i++) {
            p.getInputs().getInputs().get(i).setName(names[i]);
        }
        processData = p.toString();
    }

    /**
     * Adds optional property to process definition.
     *
     * @param properties desired properties to be added
     */
    public void addProcessProperty(Property... properties) {
        ProcessMerlin p = new ProcessMerlin(processData);
        for (Property property : properties) {
            p.getProperties().getProperties().add(property);
        }
        processData = p.toString();
    }

    /**
     * Sets partition for each input, according to number of supplied partitions.
     *
     * @param partition partitions to be set
     */
    public void setProcessInputPartition(String... partition) {
        ProcessMerlin p = new ProcessMerlin(processData);
        for (int i = 0; i < partition.length; i++) {
            p.getInputs().getInputs().get(i).setPartition(partition[i]);
        }
        processData = p.toString();
    }

    /**
     * Sets name(s) of the process output(s).
     *
     * @param names new names of the outputs
     */
    public void setProcessOutputNames(String... names) {
        ProcessMerlin p = new ProcessMerlin(processData);
        Outputs outputs = p.getOutputs();
        Assert.assertEquals(outputs.getOutputs().size(), names.length,
                "Number of output names is not equal to number of outputs in process");
        for (int i = 0; i < names.length; i++) {
            outputs.getOutputs().get(i).setName(names[i]);
        }
        p.setOutputs(outputs);
        processData = p.toString();
    }

    public void addInputFeedToBundle(String feedRefName, String feed, int templateInputIdx) {
        this.getDataSets().add(feed);
        String feedName = Util.readEntityName(feed);
        String vProcessData = getProcessData();

        ProcessMerlin processObject = new ProcessMerlin(vProcessData);
        final List<Input> processInputs = processObject.getInputs().getInputs();
        Input templateInput = processInputs.get(templateInputIdx);
        Input newInput = new Input();
        newInput.setFeed(feedName);
        newInput.setName(feedRefName);
        newInput.setOptional(templateInput.isOptional());
        newInput.setStart(templateInput.getStart());
        newInput.setEnd(templateInput.getEnd());
        newInput.setPartition(templateInput.getPartition());
        processInputs.add(newInput);
        setProcessData(processObject.toString());
    }

    public void addOutputFeedToBundle(String feedRefName, String feed, int templateOutputIdx) {
        this.getDataSets().add(feed);
        String feedName = Util.readEntityName(feed);
        ProcessMerlin processObject = getProcessObject();
        final List<Output> processOutputs = processObject.getOutputs().getOutputs();
        Output templateOutput = processOutputs.get(templateOutputIdx);
        Output newOutput = new Output();
        newOutput.setFeed(feedName);
        newOutput.setName(feedRefName);
        newOutput.setInstance(templateOutput.getInstance());
        processOutputs.add(newOutput);
        setProcessData(processObject.toString());
    }

    public void setProcessProperty(String property, String value) {
        ProcessMerlin process = new ProcessMerlin(this.getProcessData());
        process.setProperty(property, value);
        this.setProcessData(process.toString());

    }

    public String getDatasetPath() {
        FeedMerlin dataElement = new FeedMerlin(getDataSets().get(0));
        if (!dataElement.getName().contains("raaw-logs16")) {
            dataElement = new FeedMerlin(getDataSets().get(1));
        }
        return dataElement.getLocations().getLocations().get(0).getPath();
    }

    public String getInputFeedFromBundle() {
        ProcessMerlin processObject = new ProcessMerlin(getProcessData());
        for (Input input : processObject.getInputs().getInputs()) {
            for (String feed : getDataSets()) {
                if (Util.readEntityName(feed).equalsIgnoreCase(input.getFeed())) {
                    return feed;
                }
            }
        }
        return null;
    }

    public String getOutputFeedFromBundle() {
        ProcessMerlin processObject = new ProcessMerlin(getProcessData());
        for (Output output : processObject.getOutputs().getOutputs()) {
            for (String feed : getDataSets()) {
                if (Util.readEntityName(feed).equalsIgnoreCase(output.getFeed())) {
                    return feed;
                }
            }
        }
        return null;
    }

    public String getOutputFeedNameFromBundle() {
        String feedData = getOutputFeedFromBundle();
        FeedMerlin feedObject = new FeedMerlin(feedData);
        return feedObject.getName();
    }

    public String getInputFeedNameFromBundle() {
        String feedData = getInputFeedFromBundle();
        FeedMerlin feedObject = new FeedMerlin(feedData);
        return feedObject.getName();
    }

    /**
     * Sets process pipelines.
     * @param pipelines proposed pipelines
     */
    public void setProcessPipeline(String... pipelines){
        ProcessMerlin process = new ProcessMerlin(getProcessData());
        process.setPipelineTag(pipelines);
        setProcessData(process.toString());
    }

    /**
     * Set ACL of bundle's cluster.
     */
    public void setCLusterACL(String owner, String group, String permission) {
        ClusterMerlin clusterMerlin = getClusterElement();
        clusterMerlin.setACL(owner, group, permission);
        writeClusterElement(clusterMerlin);

    }

    /**
     * Set ACL of bundle's input feed.
     */
    public void setInputFeedACL(String owner, String group, String permission) {
        String feedName = getInputFeedNameFromBundle();
        FeedMerlin feedMerlin = getFeedElement(feedName);
        feedMerlin.setACL(owner, group, permission);
        writeFeedElement(feedMerlin, feedName);
    }

    /**
     * Set ACL of bundle's process.
     */
    public void setProcessACL(String owner, String group, String permission) {
        ProcessMerlin processMerlin = getProcessObject();
        processMerlin.setACL(owner, group, permission);
        setProcessData(processMerlin.toString());
    }

    /**
     * Set custom tags for a process. Key-value pairs are valid.
     */
    public void setProcessTags(String value) {
        ProcessMerlin processMerlin = getProcessObject();
        processMerlin.setTags(value);
        setProcessData(processMerlin.toString());
    }
}
