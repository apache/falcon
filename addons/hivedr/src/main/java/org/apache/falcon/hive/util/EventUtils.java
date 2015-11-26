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

package org.apache.falcon.hive.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.HiveDRArgs;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hive.hcatalog.api.repl.Command;
import org.apache.hive.hcatalog.api.repl.ReplicationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class to handle Hive events for data-mirroring.
 */
public class EventUtils {
    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final int TIMEOUT_IN_SECS = 300;
    private static final String JDBC_PREFIX = "jdbc:";
    private static final int RETRY_ATTEMPTS = 3;

    private Configuration conf = null;
    private String sourceHiveServer2Uri = null;
    private String sourceDatabase = null;
    private String sourceNN = null;
    private String sourceNNKerberosPrincipal = null;
    private String jobNN = null;
    private String jobNNKerberosPrincipal = null;
    private String targetHiveServer2Uri = null;
    private String targetStagingPath = null;
    private String targetNN = null;
    private String targetNNKerberosPrincipal = null;
    private String fullyQualifiedTargetStagingPath = null;
    private List<Path> sourceCleanUpList = null;
    private List<Path> targetCleanUpList = null;
    private static final Logger LOG = LoggerFactory.getLogger(EventUtils.class);

    private FileSystem sourceFileSystem = null;
    private FileSystem jobFileSystem = null;
    private FileSystem targetFileSystem = null;
    private Connection sourceConnection = null;
    private Connection targetConnection = null;
    private Statement sourceStatement = null;
    private Statement targetStatement = null;

    private Map<String, Long> countersMap = null;

    private List<ReplicationStatus> listReplicationStatus;

    public EventUtils(Configuration conf) {
        this.conf = conf;
        sourceHiveServer2Uri = conf.get(HiveDRArgs.SOURCE_HS2_URI.getName());
        sourceDatabase = conf.get(HiveDRArgs.SOURCE_DATABASE.getName());
        sourceNN = conf.get(HiveDRArgs.SOURCE_NN.getName());
        sourceNNKerberosPrincipal = conf.get(HiveDRArgs.SOURCE_NN_KERBEROS_PRINCIPAL.getName());
        jobNN = conf.get(HiveDRArgs.JOB_CLUSTER_NN.getName());
        jobNNKerberosPrincipal = conf.get(HiveDRArgs.JOB_CLUSTER_NN_KERBEROS_PRINCIPAL.getName());
        targetHiveServer2Uri = conf.get(HiveDRArgs.TARGET_HS2_URI.getName());
        targetStagingPath = conf.get(HiveDRArgs.TARGET_STAGING_PATH.getName())
                + File.separator + conf.get(HiveDRArgs.JOB_NAME.getName());
        targetNN = conf.get(HiveDRArgs.TARGET_NN.getName());
        targetNNKerberosPrincipal = conf.get(HiveDRArgs.TARGET_NN_KERBEROS_PRINCIPAL.getName());
        sourceCleanUpList = new ArrayList<Path>();
        targetCleanUpList = new ArrayList<Path>();
        countersMap = new HashMap<>();
    }

    public void setupConnection() throws Exception {
        Class.forName(DRIVER_NAME);
        DriverManager.setLoginTimeout(TIMEOUT_IN_SECS);
        String authTokenString = ";auth=delegationToken";
        //To bypass findbugs check, need to store empty password in Properties.
        Properties password = new Properties();
        password.put("password", "");
        String user = "";

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if (currentUser != null) {
            user = currentUser.getShortUserName();
        }

        if (conf.get(HiveDRArgs.EXECUTION_STAGE.getName())
                .equalsIgnoreCase(HiveDRUtils.ExecutionStage.EXPORT.name())) {
            String connString = JDBC_PREFIX + sourceHiveServer2Uri + "/" + sourceDatabase;
            if (StringUtils.isNotEmpty(conf.get(HiveDRArgs.SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName()))) {
                connString += authTokenString;
            }
            sourceConnection = DriverManager.getConnection(connString, user, password.getProperty("password"));
            sourceStatement = sourceConnection.createStatement();
        } else {
            String connString = JDBC_PREFIX + targetHiveServer2Uri + "/" + sourceDatabase;
            if (StringUtils.isNotEmpty(conf.get(HiveDRArgs.TARGET_HIVE2_KERBEROS_PRINCIPAL.getName()))) {
                connString += authTokenString;
            }
            targetConnection = DriverManager.getConnection(connString, user, password.getProperty("password"));
            targetStatement = targetConnection.createStatement();
        }
    }

    public void initializeFS() throws IOException {
        LOG.info("Initializing staging directory");
        fullyQualifiedTargetStagingPath = new Path(targetNN, targetStagingPath).toString();
        sourceFileSystem = FileSystem.get(FileUtils.getConfiguration(sourceNN, sourceNNKerberosPrincipal));
        jobFileSystem = FileSystem.get(FileUtils.getConfiguration(jobNN, jobNNKerberosPrincipal));
        targetFileSystem = FileSystem.get(FileUtils.getConfiguration(targetNN, targetNNKerberosPrincipal));
    }

    private String readEvents(Path eventFileName) throws IOException {
        StringBuilder eventString = new StringBuilder();
        BufferedReader in = new BufferedReader(new InputStreamReader(jobFileSystem.open(eventFileName)));
        try {
            String line;
            while ((line=in.readLine())!=null) {
                eventString.append(line);
                eventString.append(DelimiterUtils.NEWLINE_DELIM);
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }

        return eventString.toString();
    }

    public void processEvents(String event) throws Exception {
        listReplicationStatus = new ArrayList<ReplicationStatus>();
        String[] eventSplit = event.split(DelimiterUtils.FIELD_DELIM);
        String dbName = new String(Base64.decodeBase64(eventSplit[0]), "UTF-8");
        String tableName = new String(Base64.decodeBase64(eventSplit[1]), "UTF-8");
        String exportEventStr;
        String importEventStr;
        if (conf.get(HiveDRArgs.EXECUTION_STAGE.getName())
                .equalsIgnoreCase(HiveDRUtils.ExecutionStage.EXPORT.name())) {
            exportEventStr = readEvents(new Path(eventSplit[2]));
            if (StringUtils.isNotEmpty(exportEventStr)) {
                LOG.info("Process the export statements for db {} table {}", dbName, tableName);
                processCommands(exportEventStr, dbName, tableName, sourceStatement, sourceCleanUpList, false);
                if (!sourceCleanUpList.isEmpty()) {
                    invokeCopy(sourceCleanUpList);
                }
            }
        } else if (conf.get(HiveDRArgs.EXECUTION_STAGE.getName())
                .equalsIgnoreCase(HiveDRUtils.ExecutionStage.IMPORT.name())) {
            importEventStr = readEvents(new Path(eventSplit[3]));
            if (StringUtils.isNotEmpty(importEventStr)) {
                LOG.info("Process the import statements for db {} table {}", dbName, tableName);
                processCommands(importEventStr, dbName, tableName, targetStatement, targetCleanUpList, true);
            }
        }
    }

    public List<ReplicationStatus> getListReplicationStatus() {
        return listReplicationStatus;
    }

    private void processCommands(String eventStr, String dbName, String tableName, Statement sqlStmt,
                                 List<Path> cleanUpList, boolean isImportStatements)
        throws SQLException, HiveReplicationException, IOException {
        String[] commandList = eventStr.split(DelimiterUtils.NEWLINE_DELIM);
        List<Command> deserializeCommand = new ArrayList<Command>();
        for (String command : commandList) {
            Command cmd = ReplicationUtils.deserializeCommand(command);
            deserializeCommand.add(cmd);
            List<String> cleanupLocations = cmd.cleanupLocationsAfterEvent();
            cleanUpList.addAll(getCleanUpPaths(cleanupLocations));
        }
        for (Command cmd : deserializeCommand) {
            try {
                LOG.debug("Executing command : {} : {} ", cmd.getEventId(), cmd.toString());
                executeCommand(cmd, dbName, tableName, sqlStmt, isImportStatements, 0);
            } catch (Exception e) {
                // clean up locations before failing.
                cleanupEventLocations(sourceCleanUpList, sourceFileSystem);
                cleanupEventLocations(targetCleanUpList, targetFileSystem);
                throw new HiveReplicationException("Could not process replication command for "
                        + " DB Name:" + dbName + ", Table Name:" + tableName, e);
            }
        }
    }

    private void executeCommand(Command cmd, String dbName, String tableName,
                                Statement sqlStmt, boolean isImportStatements, int attempt)
        throws HiveReplicationException, SQLException, IOException {
        for (final String stmt : cmd.get()) {
            executeSqlStatement(cmd, dbName, tableName, sqlStmt, stmt, isImportStatements, attempt);
        }
        if (isImportStatements) {
            addReplicationStatus(ReplicationStatus.Status.SUCCESS, dbName, tableName, cmd.getEventId());
        }
    }

    private void executeSqlStatement(Command cmd, String dbName, String tableName,
                                     Statement sqlStmt, String stmt, boolean isImportStatements, int attempt)
        throws HiveReplicationException, SQLException, IOException {
        try {
            sqlStmt.execute(stmt);
        } catch (SQLException sqeOuter) {
            // Retry if command is retriable.
            if (attempt < RETRY_ATTEMPTS && cmd.isRetriable()) {
                if (isImportStatements) {
                    try {
                        cleanupEventLocations(getCleanUpPaths(cmd.cleanupLocationsPerRetry()), targetFileSystem);
                    } catch (IOException ioe) {
                        // Clean up failed before retry on target. Update failure status and return
                        addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName,
                                tableName, cmd.getEventId());
                        throw ioe;
                    }
                } else {
                    cleanupEventLocations(getCleanUpPaths(cmd.cleanupLocationsPerRetry()), sourceFileSystem);
                }
                executeCommand(cmd, dbName, tableName, sqlStmt, isImportStatements, ++attempt);
                return; // Retry succeeded, return without throwing an exception.
            }
            // If we reached here, retries have failed.
            LOG.error("SQL Exception: {}", sqeOuter);
            undoCommand(cmd, dbName, tableName, sqlStmt, isImportStatements);
            if (isImportStatements) {
                addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName, tableName, cmd.getEventId());
            }
            throw sqeOuter;
        }
    }

    private static List<Path> getCleanUpPaths(List<String> cleanupLocations) {
        List<Path> cleanupLocationPaths = new ArrayList<Path>();
        for (String cleanupLocation : cleanupLocations) {
            cleanupLocationPaths.add(new Path(cleanupLocation));
        }
        return cleanupLocationPaths;
    }

    private void undoCommand(Command cmd, String dbName,
                             String tableName, Statement sqlStmt, boolean isImportStatements)
        throws SQLException, HiveReplicationException {
        if (cmd.isUndoable()) {
            try {
                List<String> undoCommands = cmd.getUndo();
                LOG.debug("Undo command: {}", StringUtils.join(undoCommands.toArray()));
                if (undoCommands.size() != 0) {
                    for (final String undoStmt : undoCommands) {
                        sqlStmt.execute(undoStmt);
                    }
                }
            } catch (SQLException sqeInner) {
                if (isImportStatements) {
                    addReplicationStatus(ReplicationStatus.Status.FAILURE, dbName,
                            tableName, cmd.getEventId());
                }
                LOG.error("SQL Exception: {}", sqeInner);
                throw sqeInner;
            }
        }
    }

    private void addReplicationStatus(ReplicationStatus.Status status, String dbName, String tableName, long eventId)
        throws HiveReplicationException {
        try {
            String drJobName = conf.get(HiveDRArgs.JOB_NAME.getName());
            ReplicationStatus rs = new ReplicationStatus(conf.get(HiveDRArgs.SOURCE_CLUSTER.getName()),
                    conf.get(HiveDRArgs.TARGET_CLUSTER.getName()), drJobName, dbName, tableName, status, eventId);
            listReplicationStatus.add(rs);
        } catch (HiveReplicationException hre) {
            throw new HiveReplicationException("Could not update replication status store for "
                    + " EventId:" + eventId
                    + " DB Name:" + dbName
                    + " Table Name:" + tableName
                    + hre.toString());
        }
    }

    public void invokeCopy(List<Path> srcStagingPaths) throws Exception {
        DistCpOptions options = getDistCpOptions(srcStagingPaths);
        DistCp distCp = new DistCp(conf, options);
        LOG.info("Started DistCp with source Path: {} \ttarget path: {}", StringUtils.join(srcStagingPaths.toArray()),
                fullyQualifiedTargetStagingPath);
        Job distcpJob = distCp.execute();
        LOG.info("Distp Hadoop job: {}", distcpJob.getJobID().toString());
        LOG.info("Completed DistCp");
        if (distcpJob.getStatus().getState() == JobStatus.State.SUCCEEDED) {
            countersMap = HiveDRUtils.fetchReplicationCounters(conf, distcpJob);
        }
    }

    public DistCpOptions getDistCpOptions(List<Path> srcStagingPaths) {
        /*
         * Add the fully qualified sourceNameNode to srcStagingPath uris. This will
         * ensure DistCp will succeed when the job is run on target cluster.
         */
        List<Path> fullyQualifiedSrcStagingPaths = new ArrayList<Path>();
        for (Path srcPath : srcStagingPaths) {
            fullyQualifiedSrcStagingPaths.add(new Path(sourceNN, srcPath.toString()));
        }
        fullyQualifiedSrcStagingPaths.toArray(new Path[fullyQualifiedSrcStagingPaths.size()]);

        DistCpOptions distcpOptions = new DistCpOptions(fullyQualifiedSrcStagingPaths,
                new Path(fullyQualifiedTargetStagingPath));
        /* setSyncFolder to false to retain dir structure as in source at the target. If set to true all files will be
        copied to the same staging sir at target resulting in DuplicateFileException in DistCp.
        */

        distcpOptions.setSyncFolder(false);
        distcpOptions.setBlocking(true);
        distcpOptions.setMaxMaps(Integer.valueOf(conf.get(HiveDRArgs.DISTCP_MAX_MAPS.getName())));
        distcpOptions.setMapBandwidth(Integer.valueOf(conf.get(HiveDRArgs.DISTCP_MAP_BANDWIDTH.getName())));
        return distcpOptions;
    }

    public Long getCounterValue(String counterKey) {
        return countersMap.get(counterKey);
    }

    public boolean isCountersMapEmtpy() {
        return countersMap.size() == 0 ? true : false;
    }

    public void cleanEventsDirectory() throws IOException {
        LOG.info("Cleaning staging directory");
        cleanupEventLocations(sourceCleanUpList, sourceFileSystem);
        cleanupEventLocations(targetCleanUpList, targetFileSystem);
    }

    private void cleanupEventLocations(List<Path> cleanupList, FileSystem fileSystem)
        throws IOException {
        for (Path cleanUpPath : cleanupList) {
            try {
                fileSystem.delete(cleanUpPath, true);
            } catch (IOException ioe) {
                LOG.error("Cleaning up of staging directory {} failed {}", cleanUpPath, ioe.toString());
                throw ioe;
            }
        }

    }

    public void closeConnection() throws SQLException {
        if (sourceStatement != null) {
            sourceStatement.close();
        }

        if (targetStatement != null) {
            targetStatement.close();
        }

        if (sourceConnection != null) {
            sourceConnection.close();
        }
        if (targetConnection != null) {
            targetConnection.close();
        }
    }
}
