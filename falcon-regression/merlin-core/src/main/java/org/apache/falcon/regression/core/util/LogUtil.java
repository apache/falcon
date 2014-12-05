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

package org.apache.falcon.regression.core.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;

/**
 * Util class for managing logs.
 */
public final class LogUtil {
    private static final Logger LOGGER = Logger.getLogger(LogUtil.class);
    private static final char NL = '\n';
    private static final String HR = StringUtils.repeat("-", 80);
    private static final String HR_2 = StringUtils.repeat("-", 120);
    private static final String HR_3 = StringUtils.repeat("-", 160);

    private LogUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    private enum OozieDump {
        BundleDump {
            @Override
            void writeLogs(OozieClient oozieClient, String location, Collection<File> filter) {
                final List<BundleJob> bundleJobsInfo;
                try {
                    bundleJobsInfo = oozieClient.getBundleJobsInfo("", 0, 1000000);
                } catch (OozieClientException e) {
                    LOGGER.error("Couldn't fetch list of bundles. Exception: " + e);
                    return;
                }
                for (BundleJob oneJobInfo : bundleJobsInfo) {
                    final String bundleJobId = oneJobInfo.getId();
                    if (!skipInfo()) {
                        writeOneJobInfo(oozieClient, bundleJobId, location, filter);
                    }
                    if (!skipLog()) {
                        writeOneJobLog(oozieClient, bundleJobId, location, filter);
                    }
                }
            }

            /**
             * Pull and dump info of one job.
             * @param oozieClient oozie client that will be used for pulling log
             * @param bundleJobId job id of the bundle job
             * @param location local location where logs will be dumped
             * @param filter list of files that have already been dumped
             */
            private void writeOneJobInfo(OozieClient oozieClient, String bundleJobId,
                                         String location, Collection<File> filter) {
                final String fileName = OSUtil.getPath(location, bundleJobId + "-info.log");
                final File file = new File(fileName);
                if (filter != null && filter.contains(file)) {
                    return;
                }
                final BundleJob info;
                try {
                    info = oozieClient.getBundleJobInfo(bundleJobId);
                } catch (OozieClientException e) {
                    LOGGER.error("Couldn't fetch bundle info for " + bundleJobId + ". "
                        + "Exception: " + e);
                    return;
                }
                StringBuilder sb = new StringBuilder();
                sb.append("Bundle ID : ").append(info.getId()).append(NL);
                sb.append(HR).append(NL);
                sb.append("Bundle Name : ").append(info.getAppName()).append(NL);
                sb.append("App Path : ").append(info.getAppPath()).append(NL);
                sb.append("Status : ").append(info.getStatus()).append(NL);
                sb.append("User : ").append(info.getUser()).append(NL);
                sb.append("Created : ").append(info.getCreatedTime()).append(NL);
                sb.append("Started : ").append(info.getStartTime()).append(NL);
                sb.append("EndTime : ").append(info.getEndTime()).append(NL);
                sb.append("Kickoff time : ").append(info.getKickoffTime()).append(NL);
                sb.append(HR_2).append(NL);
                final String format = "%-40s %-10s %-5s %-10s %-30s %-20s";
                sb.append(String.format(format,
                    "Job ID", "Status", "Freq", "Unit", "Started", "Next Materialized")).append(NL);
                sb.append(HR_2).append(NL);
                for (CoordinatorJob cj : info.getCoordinators()) {
                    sb.append(String.format(format,
                        cj.getId(), cj.getStatus(),  cj.getFrequency(), cj.getTimeUnit(), cj.getStartTime(),
                        cj.getNextMaterializedTime())).append(NL);
                }
                sb.append(HR_2).append(NL);
                try {
                    FileUtils.writeStringToFile(file, sb.toString());
                } catch (IOException e) {
                    LOGGER.error("Couldn't write bundle info for " + bundleJobId + ". "
                        + "Exception: " + e);
                }
            }
        },

        CoordDump {
            @Override
            void writeLogs(OozieClient oozieClient, String location, Collection<File> filter) {
                final List<CoordinatorJob> coordJobsInfo;
                try {
                    coordJobsInfo = oozieClient.getCoordJobsInfo("", 0, 1000000);
                } catch (OozieClientException e) {
                    LOGGER.error("Couldn't fetch list of bundles. Exception: " + e);
                    return;
                }
                for (CoordinatorJob oneJobInfo : coordJobsInfo) {
                    final String coordJobId = oneJobInfo.getId();
                    if (!skipInfo()) {
                        writeOneJobInfo(oozieClient, coordJobId, location, filter);
                    }
                    if (!skipLog()) {
                        writeOneJobLog(oozieClient, coordJobId, location, filter);
                    }
                }
            }

            /**
             * Pull and dump info of one job.
             * @param oozieClient oozie client that will be used for pulling log
             * @param coordJobId job id of the coordinator job
             * @param location local location where logs will be dumped
             * @param filter list of files that have already been dumped
             */
            private void writeOneJobInfo(OozieClient oozieClient, String coordJobId,
                                         String location, Collection<File> filter) {
                final String fileName = OSUtil.getPath(location, coordJobId + "-info.log");
                final File file = new File(fileName);
                if (filter != null && filter.contains(file)) {
                    return;
                }
                final CoordinatorJob info;
                try {
                    info = oozieClient.getCoordJobInfo(coordJobId);
                } catch (OozieClientException e) {
                    LOGGER.error("Couldn't fetch bundle info for " + coordJobId + ". "
                        + "Exception: " + e);
                    return;
                }
                StringBuilder sb = new StringBuilder();
                sb.append("Coordinator Job ID : ").append(info.getId()).append(NL);
                sb.append(HR).append(NL);
                sb.append("Job Name : ").append(info.getAppName()).append(NL);
                sb.append("App Path : ").append(info.getAppPath()).append(NL);
                sb.append("Status : ").append(info.getStatus()).append(NL);
                sb.append("User : ").append(info.getUser()).append(NL);
                sb.append("Started : ").append(info.getStartTime()).append(NL);
                sb.append("EndTime : ").append(info.getEndTime()).append(NL);
                sb.append(HR_3).append(NL);
                final String format = "%-40s %-10s %-40s %-10s %-30s %-30s";
                sb.append(String.format(format,
                    "Job ID", "Status", "Ext ID", "Err Code", "Created",
                    "Nominal Time")).append(NL);
                sb.append(HR_3).append(NL);
                for (CoordinatorAction cj : info.getActions()) {
                    sb.append(String.format(format,
                        cj.getId(), cj.getStatus(),  cj.getExternalId(), cj.getErrorCode(),
                        cj.getCreatedTime(), cj.getNominalTime())).append(NL);
                }
                sb.append(HR_3).append(NL);
                try {
                    FileUtils.writeStringToFile(file, sb.toString());
                } catch (IOException e) {
                    LOGGER.error("Couldn't write coord job info for " + coordJobId + ". "
                        + "Exception: " + e);
                }
            }
        },

        WfDump {
            @Override
            void writeLogs(OozieClient oozieClient, String location, Collection<File> filter) {
                final List<WorkflowJob> wfJobsInfo;
                try {
                    wfJobsInfo = oozieClient.getJobsInfo("", 0, 1000000);
                } catch (OozieClientException e) {
                    LOGGER.error("Couldn't fetch list of bundles. Exception: " + e);
                    return;
                }
                for (WorkflowJob oneJobInfo : wfJobsInfo) {
                    final String wfJobId = oneJobInfo.getId();
                    if (!skipInfo()) {
                        writeOneJobInfo(oozieClient, wfJobId, location, filter);
                    }
                    if (!skipLog()) {
                        writeOneJobLog(oozieClient, wfJobId, location, filter);
                    }
                }
            }

            /**
             * Pull and dump info of one job.
             * @param oozieClient oozie client that will be used for pulling log
             * @param wfJobId job id of the workflow job
             * @param location local location where logs will be dumped
             * @param filter list of files that have already been dumped
             */
            private void writeOneJobInfo(OozieClient oozieClient, String wfJobId,
                                         String location, Collection<File> filter) {
                final String fileName = OSUtil.getPath(location, wfJobId + "-info.log");
                final File file = new File(fileName);
                if (filter != null && filter.contains(file)) {
                    return;
                }
                final WorkflowJob info;
                try {
                    info = oozieClient.getJobInfo(wfJobId);
                } catch (OozieClientException e) {
                    LOGGER.error("Couldn't fetch bundle info for " + wfJobId + ". Exception: " + e);
                    return;
                }
                StringBuilder sb = new StringBuilder();
                sb.append("Workflow Job ID : ").append(info.getId()).append(NL);
                sb.append(HR).append(NL);
                sb.append("Wf Name : ").append(info.getAppName()).append(NL);
                sb.append("App Path : ").append(info.getAppPath()).append(NL);
                sb.append("Status : ").append(info.getStatus()).append(NL);
                sb.append("Run : ").append(info.getRun()).append(NL);
                sb.append("User : ").append(info.getUser()).append(NL);
                sb.append("Group : ").append(info.getAcl()).append(NL);
                sb.append("Created : ").append(info.getCreatedTime()).append(NL);
                sb.append("Started : ").append(info.getStartTime()).append(NL);
                sb.append("Last Modified : ").append(info.getLastModifiedTime()).append(NL);
                sb.append("EndTime : ").append(info.getEndTime()).append(NL);
                sb.append("External ID : ").append(info.getExternalId()).append(NL);
                sb.append(NL).append("Actions").append(NL);
                sb.append(HR_3).append(NL);
                final String format = "%-80s %-10s %-40s %-15s %-10s";
                sb.append(String.format(format,
                    "Job ID", "Status", "Ext ID", "Ext Status", "Err Code")).append(NL);
                sb.append(HR_3).append(NL);
                for (WorkflowAction cj : info.getActions()) {
                    sb.append(String.format(format,
                        cj.getId(), cj.getStatus(),  cj.getExternalId(), cj.getExternalStatus(),
                        cj.getErrorCode())).append(NL);
                }
                sb.append(HR_3).append(NL);
                try {
                    FileUtils.writeStringToFile(file, sb.toString());
                } catch (IOException e) {
                    LOGGER.error("Couldn't write wf job info for " + wfJobId + ". Exception: " + e);
                }
            }
        };

        private static boolean skipInfo() {
            return Config.getBoolean("log.capture.oozie.skip_info", false);
        }

        private static boolean skipLog() {
            return Config.getBoolean("log.capture.oozie.skip_log", false);
        }

        /**
         * Pull and dump info and log of all jobs of a type.
         * @param oozieClient oozie client that will be used for pulling log
         * @param location local location where logs will be dumped
         * @param filter list of files that have already been dumped
         */
        abstract void writeLogs(OozieClient oozieClient, String location, Collection<File> filter);

        /**
         * Pull and dump log of one job.
         * @param oozieClient oozie client that will be used for pulling log
         * @param jobId job id of the job
         * @param location local location where logs will be dumped
         * @param filter list of files that have already been dumped
         */
        private static void writeOneJobLog(OozieClient oozieClient, String jobId,
            String location, Collection<File> filter) {
            final String fileName = OSUtil.getPath(location, jobId + ".log");
            assert fileName != null;
            final File file = new File(fileName);
            if (filter != null && filter.contains(file)) {
                return;
            }
            try {
                oozieClient.getJobLog(jobId, "", "", new PrintStream(file));
            } catch (OozieClientException e) {
                LOGGER.error("Couldn't fetch log for " + jobId + ". Exception: " + e);
            } catch (FileNotFoundException e) {
                LOGGER.error("Couldn't write log for " + jobId + ". Exception: " + e);
            }
        }
    }

    /**
     * Pulls and dumps oozie logs at a configured location.
     * @param coloHelper coloHelper of the cluster from which oozie logs are going to be pulled
     * @param logLocation local location at which logs are going to be dumped
     */
    public static void writeOozieLogs(ColoHelper coloHelper, String logLocation) {
        final OozieClient oozieClient = coloHelper.getFeedHelper().getOozieClient();
        final String hostname = coloHelper.getClusterHelper().getQaHost();
        final String oozieLogLocation = OSUtil.getPath(logLocation + "oozie_logs", hostname);
        assert oozieLogLocation != null;
        final File directory = new File(oozieLogLocation);
        if (!directory.exists()) {
            try {
                FileUtils.forceMkdir(directory);
            } catch (IOException e) {
                LOGGER.error("Directory creation failed for: " + directory + ". Exception: " + e);
                return;
            }
        }
        final Collection<File> filter = FileUtils.listFiles(directory, null, true);
        for (OozieDump oozieDump : OozieDump.values()) {
            oozieDump.writeLogs(oozieClient, oozieLogLocation, filter);
        }
    }

}
