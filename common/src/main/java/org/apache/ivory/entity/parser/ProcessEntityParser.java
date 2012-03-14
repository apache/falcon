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

package org.apache.ivory.entity.parser;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.common.ELParser;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.entity.v0.process.Validity;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.SyncCoordDataset;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.IOUtils;

/**
 * Concrete Parser which has XML parsing and validation logic for Process XML.
 * 
 */
public class ProcessEntityParser extends EntityParser<Process> {

    private static final Logger LOG = Logger.getLogger(ProcessEntityParser.class);

    public ProcessEntityParser() {
        super(EntityType.PROCESS);
    }

    public static void init() throws IvoryException {
        String uri = StartupProperties.get().getProperty("config.oozie.conf.uri");
        System.setProperty(Services.OOZIE_HOME_DIR, uri);
        File confFile = new File(uri + "/conf");
        if (!confFile.exists() && !confFile.mkdirs())
            throw new IvoryException("Failed to create conf directory in path " + uri);

        InputStream instream = ProcessEntityParser.class.getResourceAsStream("/oozie-site.xml");
        try {
            IOUtils.copyStream(instream, new FileOutputStream(uri + "/conf/oozie-site.xml"));
            Services services = new Services();
            services.getConf().set("oozie.services", "org.apache.oozie.service.ELService");
            services.init();
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    @Override
    public void validate(Process process) throws IvoryException {
        // check if dependent entities exists
        String clusterName = process.getCluster().getName();
        validateEntityExists(EntityType.CLUSTER, clusterName);
        validateProcessValidity(process.getValidity().getStart(), process.getValidity().getEnd());
        validateHDFSpaths(process);

        if (process.getInputs() != null && process.getInputs().getInput() != null)
            for (Input input : process.getInputs().getInput()) {
                validateEntityExists(EntityType.FEED, input.getFeed());
                validateFeedDefinedForCluster(input.getFeed(), clusterName);
                // TODO currently retention supports deletion of past instances
                // only
                // hence checking for only startinstance of input
                validateFeedRetentionPeriod(input.getStartInstance(), input.getFeed(), clusterName);
                validateInstanceRange(process, input);
            }

        if (process.getOutputs() != null && process.getOutputs().getOutput() != null)
            for (Output output : process.getOutputs().getOutput()) {
                validateEntityExists(EntityType.FEED, output.getFeed());
                validateFeedDefinedForCluster(output.getFeed(), clusterName);
                validateInstance(process, output);
            }

        // validate partitions
        if (process.getInputs() != null && process.getInputs().getInput() != null)
            for (Input input : process.getInputs().getInput()) {
                if (input.getPartition() != null) {
                    String partition = input.getPartition();
                    String[] parts = partition.split("/");
                    Feed feed = ConfigurationStore.get().get(EntityType.FEED, input.getFeed());
                    if (feed.getPartitions() == null || feed.getPartitions().getPartition() == null
                            || feed.getPartitions().getPartition().size() == 0
                            || feed.getPartitions().getPartition().size() < parts.length)
                        throw new ValidationException("Partition specification in input " + input.getName() + " is wrong");
                }
            }
    }

    private void validateHDFSpaths(Process process) throws IvoryException {

        String clusterName = process.getCluster().getName();
        org.apache.ivory.entity.v0.cluster.Cluster cluster = (org.apache.ivory.entity.v0.cluster.Cluster) ConfigurationStore.get()
                .get(EntityType.CLUSTER, clusterName);
        String workflowPath = process.getWorkflow().getPath();
        String nameNode = getNameNode(cluster, clusterName);
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name", nameNode);
            FileSystem fs = FileSystem.get(configuration);
            if (!fs.exists(new Path(workflowPath))) {
                throw new ValidationException("Workflow path: " + workflowPath + " does not exists in HDFS: " + nameNode);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e);
        } catch (ConnectException e) {
            throw new ValidationException("Unable to connect to Namenode: " + nameNode + " referenced in cluster: " + clusterName);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    private String getNameNode(org.apache.ivory.entity.v0.cluster.Cluster cluster, String clusterName) throws ValidationException {
        // cluster should never be null as it is validated while submitting
        // feeds.
        if (!ClusterHelper.getHdfsUrl(cluster).startsWith("hdfs://")) {
            throw new ValidationException("Cannot get valid nameNode from write interface of cluster: " + clusterName);
        }
        return ClusterHelper.getHdfsUrl(cluster);
    }

    private void validateFeedRetentionPeriod(String startInstance, String feedName, String clusterName) throws IvoryException {
        Feed feed = (Feed) ConfigurationStore.get().get(EntityType.FEED, feedName);
        String feedRetention = feed.getCluster(clusterName).getRetention().getLimit();
        ELParser elParser = new ELParser();
        elParser.parseElExpression(startInstance);
        long requiredInputDuration = elParser.getRequiredInputDuration();
        elParser.parseOozieELExpression(feedRetention);
        long feedDuration = elParser.getFeedDuration();

        if (feedDuration - requiredInputDuration < 0) {
            throw new ValidationException("StartInstance :" + startInstance + " of process is out of range for Feed: " + feedName
                    + "  in cluster: " + clusterName + "'s retention limit :" + feedRetention);
        }
    }

    private void validateProcessValidity(String start, String end) throws IvoryException {
        try {
            validateProcessDates(start, end);
            Date processStart = EntityUtil.parseDateUTC(start);
            Date processEnd = EntityUtil.parseDateUTC(end);
            if (processStart.after(processEnd)) {
                throw new ValidationException("Process start time: " + start + " cannot be after process end time: " + end);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    private void validateFeedDefinedForCluster(String feedName, String clusterName) throws IvoryException {
        Feed feed = (Feed) ConfigurationStore.get().get(EntityType.FEED, feedName);
        if (feed.getCluster(clusterName) == null)
            throw new ValidationException("Feed " + feed.getName() + " is not defined for cluster " + clusterName);
    }

    private void configEvaluator(SyncCoordDataset ds, SyncCoordAction appInst, ELEvaluator eval, Feed feed, String clusterName,
            Validity procValidity) throws IvoryException {
        try {
            Cluster cluster = feed.getCluster(clusterName);
            ds.setInitInstance(EntityUtil.parseDateUTC(cluster.getValidity().getStart()));
            ds.setFrequency(feed.getPeriodicity());
            ds.setTimeUnit(Frequency.valueOf(feed.getFrequency()).timeUnit);
            ds.setEndOfDuration(Frequency.valueOf(feed.getFrequency()).endOfDuration);
            ds.setTimeZone(DateUtils.getTimeZone(cluster.getValidity().getTimezone()));
            ds.setName(feed.getName());
            ds.setUriTemplate(feed.getLocations().get(LocationType.DATA).getPath());
            ds.setType("SYNC");
            ds.setDoneFlag("");

            appInst.setActualTime(EntityUtil.parseDateUTC(procValidity.getStart()));
            appInst.setNominalTime(EntityUtil.parseDateUTC(procValidity.getStart()));
            appInst.setTimeZone(EntityUtil.getTimeZone(procValidity.getTimezone()));
            appInst.setActionId("porcess@1");
            appInst.setName("process");

            eval.setVariable(OozieClient.USER_NAME, "test_user");
            eval.setVariable(OozieClient.GROUP_NAME, "test_group");
            CoordELFunctions.configureEvaluator(eval, ds, appInst);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    // Mapping to oozie coord's dataset fields
    public static enum Frequency {
        minutes(TimeUnit.MINUTE, TimeUnit.NONE), hours(TimeUnit.HOUR, TimeUnit.NONE), days(TimeUnit.DAY, TimeUnit.NONE), months(
                TimeUnit.MONTH, TimeUnit.NONE), endOfDays(TimeUnit.DAY, TimeUnit.END_OF_DAY), endOfMonths(TimeUnit.MONTH,
                TimeUnit.END_OF_MONTH);

        private TimeUnit timeUnit;
        private TimeUnit endOfDuration;

        private Frequency(TimeUnit timeUnit, TimeUnit endOfDuration) {
            this.timeUnit = timeUnit;
            this.endOfDuration = endOfDuration;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }
    }

    private void validateInstance(Process process, Output output) throws IvoryException {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-action-create");
        SyncCoordDataset ds = new SyncCoordDataset();
        SyncCoordAction appInst = new SyncCoordAction();
        Feed feed = ConfigurationStore.get().get(EntityType.FEED, output.getFeed());
        String clusterName = process.getCluster().getName();
        configEvaluator(ds, appInst, eval, feed, clusterName, process.getValidity());

        try {
            org.apache.ivory.entity.v0.feed.Validity feedValidity = feed.getCluster(clusterName).getValidity();
            Date feedEnd = EntityUtil.parseDateUTC(feedValidity.getEnd());

            String instEL = output.getInstance();
            String instStr = CoordELFunctions.evalAndWrap(eval, "${elext:" + instEL + "}");
            if (instStr.equals(""))
                throw new ValidationException("Instance  " + instEL + " of feed " + feed.getName() + " is before the start of feed "
                        + feedValidity.getStart());

            Date inst = EntityUtil.parseDateUTC(instStr);
            if (inst.after(feedEnd))
                throw new ValidationException("End instance " + instEL + " for feed " + feed.getName() + " is after the end of feed "
                        + feedValidity.getEnd());
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    private void validateInstanceRange(Process process, Input input) throws IvoryException {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-action-create");
        SyncCoordDataset ds = new SyncCoordDataset();
        SyncCoordAction appInst = new SyncCoordAction();
        Feed feed = ConfigurationStore.get().get(EntityType.FEED, input.getFeed());
        String clusterName = process.getCluster().getName();
        configEvaluator(ds, appInst, eval, feed, clusterName, process.getValidity());

        try {

            org.apache.ivory.entity.v0.feed.Validity feedValidity = feed.getCluster(clusterName).getValidity();
            Date feedEnd = EntityUtil.parseDateUTC(feedValidity.getEnd());

            String instStartEL = input.getStartInstance();
            String instEndEL = input.getEndInstance();

            String instStartStr = CoordELFunctions.evalAndWrap(eval, "${elext:" + instStartEL + "}");
            if (instStartStr.equals(""))
                throw new ValidationException("Start instance  " + instStartEL + " of feed " + feed.getName()
                        + " is before the start of feed " + feedValidity.getStart());

            String instEndStr = CoordELFunctions.evalAndWrap(eval, "${elext:" + instEndEL + "}");
            if (instEndStr.equals(""))
                throw new ValidationException("End instance  " + instEndEL + " of feed " + feed.getName()
                        + " is before the start of feed " + feedValidity.getStart());

            Date instStart = EntityUtil.parseDateUTC(instStartStr);
            Date instEnd = EntityUtil.parseDateUTC(instEndStr);
            if (instEnd.before(instStart))
                throw new ValidationException("End instance " + instEndEL + " for feed " + feed.getName()
                        + " is before the start instance " + instStartEL);

            if (instEnd.after(feedEnd))
                throw new ValidationException("End instance " + instEndEL + " for feed " + feed.getName()
                        + " is after the end of feed " + feedValidity.getEnd());
        } catch (ValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new IvoryException(e);
        }

    }

    private void validateProcessDates(String start, String end) throws ValidationException {
        if (!EntityUtil.isValidUTCDate(start)) {
            throw new ValidationException("Invalid start date: " + start);
        }
        if (!EntityUtil.isValidUTCDate(end)) {
            throw new ValidationException("Invalid end date: " + end);
        }

    }
}
