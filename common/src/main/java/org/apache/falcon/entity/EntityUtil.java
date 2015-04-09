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

package org.apache.falcon.entity;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.WorkflowNameBuilder.WorkflowName;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.*;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Helper to get entity object.
 */
public final class EntityUtil {
    public static final Logger LOG = LoggerFactory.getLogger(EntityUtil.class);

    private static final long MINUTE_IN_MS = 60 * 1000L;
    private static final long HOUR_IN_MS = 60 * MINUTE_IN_MS;
    private static final long DAY_IN_MS = 24 * HOUR_IN_MS;
    private static final long MONTH_IN_MS = 31 * DAY_IN_MS;

    public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";

    private EntityUtil() {}

    public static <T extends Entity> T getEntity(EntityType type, String entityName) throws FalconException {
        ConfigurationStore configStore = ConfigurationStore.get();
        T entity = configStore.get(type, entityName);
        if (entity == null) {
            throw new EntityNotRegisteredException(entityName + " (" + type + ") not found");
        }
        return entity;
    }

    public static <T extends Entity> T getEntity(String type, String entityName) throws FalconException {
        EntityType entityType;
        try {
            entityType = EntityType.getEnum(type);
        } catch (IllegalArgumentException e) {
            throw new FalconException("Invalid entity type: " + type, e);
        }
        return getEntity(entityType, entityName);
    }

    public static TimeZone getTimeZone(String tzId) {
        if (tzId == null) {
            throw new IllegalArgumentException("Invalid TimeZone: Cannot be null.");
        }
        TimeZone tz = TimeZone.getTimeZone(tzId);
        if (!tzId.equals("GMT") && tz.getID().equals("GMT")) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        return tz;
    }

    public static Date getEndTime(Entity entity, String cluster) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            return getEndTime((Process) entity, cluster);
        } else {
            return getEndTime((Feed) entity, cluster);
        }
    }

    public static Date parseDateUTC(String dateStr) throws FalconException {
        try {
            return SchemaHelper.parseDateUTC(dateStr);
        } catch (Exception e) {
            throw new FalconException(e);
        }
    }

    public static Date getStartTime(Entity entity, String cluster) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            return getStartTime((Process) entity, cluster);
        } else {
            return getStartTime((Feed) entity, cluster);
        }
    }

    public static Date getEndTime(Process process, String cluster) {
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
        return processCluster.getValidity().getEnd();
    }

    public static Date getStartTime(Process process, String cluster) {
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
        return processCluster.getValidity().getStart();
    }

    public static Date getEndTime(Feed feed, String cluster) {
        org.apache.falcon.entity.v0.feed.Cluster clusterDef = FeedHelper.getCluster(feed, cluster);
        return clusterDef.getValidity().getEnd();
    }

    public static Date getStartTime(Feed feed, String cluster) {
        org.apache.falcon.entity.v0.feed.Cluster clusterDef = FeedHelper.getCluster(feed, cluster);
        return clusterDef.getValidity().getStart();
    }

    public static int getParallel(Entity entity) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            return getParallel((Process) entity);
        } else {
            return getParallel((Feed) entity);
        }
    }

    public static void setStartDate(Entity entity, String cluster, Date startDate) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            setStartDate((Process) entity, cluster, startDate);
        } else {
            setStartDate((Feed) entity, cluster, startDate);
        }
    }

    public static void setEndTime(Entity entity, String cluster, Date endDate) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            setEndTime((Process) entity, cluster, endDate);
        } else {
            setEndTime((Feed) entity, cluster, endDate);
        }
    }

    public static void setParallel(Entity entity, int parallel) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            setParallel((Process) entity, parallel);
        } else {
            setParallel((Feed) entity, parallel);
        }
    }

    public static int getParallel(Process process) {
        return process.getParallel();
    }

    public static void setStartDate(Process process, String cluster, Date startDate) {
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
        processCluster.getValidity().setStart(startDate);
    }

    public static void setParallel(Process process, int parallel) {
        process.setParallel(parallel);
    }

    public static void setEndTime(Process process, String cluster, Date endDate) {
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
        processCluster.getValidity().setEnd(endDate);
    }

    public static int getParallel(Feed feed) {
        // todo - how this this supposed to work?
        return 1;
    }

    public static void setStartDate(Feed feed, String cluster, Date startDate) {
        org.apache.falcon.entity.v0.feed.Cluster clusterDef = FeedHelper.getCluster(feed, cluster);
        clusterDef.getValidity().setStart(startDate);
    }

    public static void setEndTime(Feed feed, String cluster, Date endDate) {
        org.apache.falcon.entity.v0.feed.Cluster clusterDef = FeedHelper.getCluster(feed, cluster);
        clusterDef.getValidity().setStart(endDate);
    }

    public static void setParallel(Feed feed, int parallel) {
    }

    public static Frequency getFrequency(Entity entity) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            return getFrequency((Process) entity);
        } else {
            return getFrequency((Feed) entity);
        }
    }

    public static Frequency getFrequency(Process process) {
        return process.getFrequency();
    }

    public static Frequency getFrequency(Feed feed) {
        return feed.getFrequency();
    }

    public static TimeZone getTimeZone(Entity entity) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            return getTimeZone((Process) entity);
        } else {
            return getTimeZone((Feed) entity);
        }
    }

    public static TimeZone getTimeZone(Process process) {
        return process.getTimezone();
    }

    public static TimeZone getTimeZone(Feed feed) {
        return feed.getTimezone();
    }

    public static Date getNextStartTime(Date startTime, Frequency frequency, TimeZone timezone, Date now) {
        if (startTime.after(now)) {
            return startTime;
        }

        Calendar startCal = Calendar.getInstance(timezone);
        startCal.setTime(startTime);

        int count = 0;
        switch (frequency.getTimeUnit()) {
        case months:
            count = (int) ((now.getTime() - startTime.getTime()) / MONTH_IN_MS);
            break;
        case days:
            count = (int) ((now.getTime() - startTime.getTime()) / DAY_IN_MS);
            break;
        case hours:
            count = (int) ((now.getTime() - startTime.getTime()) / HOUR_IN_MS);
            break;
        case minutes:
            count = (int) ((now.getTime() - startTime.getTime()) / MINUTE_IN_MS);
            break;
        default:
        }

        final int freq = frequency.getFrequencyAsInt();
        if (count > 2) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(), ((count - 2) / freq) * freq);
        }
        while (startCal.getTime().before(now)) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(), freq);
        }
        return startCal.getTime();
    }

    public static int getInstanceSequence(Date startTime, Frequency frequency, TimeZone tz, Date instanceTime) {
        if (startTime.after(instanceTime)) {
            return -1;
        }

        Calendar startCal = Calendar.getInstance(tz);
        startCal.setTime(startTime);

        int count = 0;
        switch (frequency.getTimeUnit()) {
        case months:
            count = (int) ((instanceTime.getTime() - startTime.getTime()) / MONTH_IN_MS);
            break;
        case days:
            count = (int) ((instanceTime.getTime() - startTime.getTime()) / DAY_IN_MS);
            break;
        case hours:
            count = (int) ((instanceTime.getTime() - startTime.getTime()) / HOUR_IN_MS);
            break;
        case minutes:
            count = (int) ((instanceTime.getTime() - startTime.getTime()) / MINUTE_IN_MS);
            break;
        default:
        }

        final int freq = frequency.getFrequencyAsInt();
        if (count > 2) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(), (count / freq) * freq);
            count = (count / freq);
        } else {
            count = 0;
        }
        while (startCal.getTime().before(instanceTime)) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(), freq);
            count++;
        }
        return count + 1;
    }

    public static Date getNextInstanceTime(Date instanceTime, Frequency frequency, TimeZone tz, int instanceCount) {
        if (tz == null) {
            tz = TimeZone.getTimeZone("UTC");
        }
        Calendar insCal = Calendar.getInstance(tz);
        insCal.setTime(instanceTime);

        final int freq = frequency.getFrequencyAsInt() * instanceCount;
        insCal.add(frequency.getTimeUnit().getCalendarUnit(), freq);

        return insCal.getTime();
    }

    public static String md5(Entity entity) throws FalconException {
        return new String(Hex.encodeHex(DigestUtils.md5(stringOf(entity))));
    }

    public static boolean equals(Entity lhs, Entity rhs) throws FalconException {
        return equals(lhs, rhs, null);
    }

    public static boolean equals(Entity lhs, Entity rhs, String[] filterProps) throws FalconException {
        if (lhs == null && rhs == null) {
            return true;
        }
        if (lhs == null || rhs == null) {
            return false;
        }

        if (lhs.equals(rhs)) {
            String lhsString = stringOf(lhs, filterProps);
            String rhsString = stringOf(rhs, filterProps);
            return lhsString.equals(rhsString);
        } else {
            return false;
        }
    }

    public static String stringOf(Entity entity) throws FalconException {
        return stringOf(entity, null);
    }

    private static String stringOf(Entity entity, String[] filterProps) throws FalconException {
        Map<String, String> map = new HashMap<String, String>();
        mapToProperties(entity, null, map, filterProps);
        List<String> keyList = new ArrayList<String>(map.keySet());
        Collections.sort(keyList);
        StringBuilder builer = new StringBuilder();
        for (String key : keyList) {
            builer.append(key).append('=').append(map.get(key)).append('\n');
        }
        return builer.toString();
    }

    @SuppressWarnings("rawtypes")
    private static void mapToProperties(Object obj, String name, Map<String, String> propMap, String[] filterProps)
        throws FalconException {

        if (obj == null) {
            return;
        }

        if (filterProps != null && name != null) {
            for (String filter : filterProps) {
                if (name.matches(filter.replace(".", "\\.").replace("[", "\\[").replace("]", "\\]"))) {
                    return;
                }
            }
        }

        if (Date.class.isAssignableFrom(obj.getClass())) {
            propMap.put(name, SchemaHelper.formatDateUTC((Date) obj));
        } else if (obj.getClass().getPackage().getName().equals("java.lang")) {
            propMap.put(name, String.valueOf(obj));
        } else if (TimeZone.class.isAssignableFrom(obj.getClass())) {
            propMap.put(name, ((TimeZone) obj).getID());
        } else if (Enum.class.isAssignableFrom(obj.getClass())) {
            propMap.put(name, ((Enum) obj).name());
        } else if (List.class.isAssignableFrom(obj.getClass())) {
            List list = (List) obj;
            for (int index = 0; index < list.size(); index++) {
                mapToProperties(list.get(index), name + "[" + index + "]", propMap, filterProps);
            }
        } else {
            try {
                Method method = obj.getClass().getDeclaredMethod("toString");
                propMap.put(name, (String) method.invoke(obj));
            } catch (NoSuchMethodException e) {
                try {
                    Map map = PropertyUtils.describe(obj);
                    for (Object entry : map.entrySet()) {
                        String key = (String)((Map.Entry)entry).getKey();
                        if (!key.equals("class")) {
                            mapToProperties(map.get(key), name != null ? name + "." + key : key, propMap,
                                    filterProps);
                        }
                    }
                } catch (Exception e1) {
                    throw new FalconException(e1);
                }
            } catch (Exception e) {
                throw new FalconException(e);
            }
        }
    }

    public static WorkflowName getWorkflowName(Tag tag, List<String> suffixes,
                                               Entity entity) {
        WorkflowNameBuilder<Entity> builder = new WorkflowNameBuilder<Entity>(
                entity);
        builder.setTag(tag);
        builder.setSuffixes(suffixes);
        return builder.getWorkflowName();
    }

    public static WorkflowName getWorkflowName(Tag tag, Entity entity) {
        return getWorkflowName(tag, null, entity);
    }

    public static WorkflowName getWorkflowName(Entity entity) {
        return getWorkflowName(null, null, entity);
    }

    public static String getWorkflowNameSuffix(String workflowName,
                                               Entity entity) throws FalconException {
        WorkflowNameBuilder<Entity> builder = new WorkflowNameBuilder<Entity>(
                entity);
        return builder.getWorkflowSuffixes(workflowName).replaceAll("_", "");
    }

    public static Tag getWorkflowNameTag(String workflowName, Entity entity) {
        WorkflowNameBuilder<Entity> builder = new WorkflowNameBuilder<Entity>(
                entity);
        return builder.getWorkflowTag(workflowName);
    }

    public static List<String> getWorkflowNames(Entity entity) {
        switch(entity.getEntityType()) {
        case FEED:
            return Arrays.asList(getWorkflowName(Tag.RETENTION, entity).toString(),
                getWorkflowName(Tag.REPLICATION, entity).toString());

        case PROCESS:
            return Arrays.asList(getWorkflowName(Tag.DEFAULT, entity).toString());

        default:
        }
        throw new IllegalArgumentException("Unhandled type: " + entity.getEntityType());
    }

    public static <T extends Entity> T getClusterView(T entity, String clusterName) {
        switch (entity.getEntityType()) {
        case CLUSTER:
            return entity;

        case FEED:
            Feed feed = (Feed) entity.copy();
            org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, clusterName);
            Iterator<org.apache.falcon.entity.v0.feed.Cluster> itr = feed.getClusters().getClusters().iterator();
            while (itr.hasNext()) {
                org.apache.falcon.entity.v0.feed.Cluster cluster = itr.next();
                //In addition to retaining the required clster, retain the sources clusters if this is the target
                // cluster
                //1. Retain cluster if cluster n
                if (!(cluster.getName().equals(clusterName)
                        || (feedCluster.getType() == ClusterType.TARGET
                        && cluster.getType() == ClusterType.SOURCE))) {
                    itr.remove();
                }
            }
            return (T) feed;

        case PROCESS:
            Process process = (Process) entity.copy();
            Iterator<org.apache.falcon.entity.v0.process.Cluster> procItr =
                process.getClusters().getClusters().iterator();
            while (procItr.hasNext()) {
                org.apache.falcon.entity.v0.process.Cluster cluster = procItr.next();
                if (!cluster.getName().equals(clusterName)) {
                    procItr.remove();
                }
            }
            return (T) process;
        default:
        }
        throw new UnsupportedOperationException("Not supported for entity type " + entity.getEntityType());
    }

    public static Set<String> getClustersDefined(Entity entity) {
        Set<String> clusters = new HashSet<String>();
        switch (entity.getEntityType()) {
        case CLUSTER:
            clusters.add(entity.getName());
            break;

        case FEED:
            Feed feed = (Feed) entity;
            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed.getClusters().getClusters()) {
                clusters.add(cluster.getName());
            }
            break;

        case PROCESS:
            Process process = (Process) entity;
            for (org.apache.falcon.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
                clusters.add(cluster.getName());
            }
            break;
        default:
        }
        return clusters;
    }

    public static Set<String> getClustersDefinedInColos(Entity entity) {
        Set<String> entityClusters = EntityUtil.getClustersDefined(entity);
        if (DeploymentUtil.isEmbeddedMode()) {
            return entityClusters;
        }

        Set<String> myClusters = DeploymentUtil.getCurrentClusters();
        Set<String> applicableClusters = new HashSet<String>();
        for (String cluster : entityClusters) {
            if (myClusters.contains(cluster)) {
                applicableClusters.add(cluster);
            }
        }
        return applicableClusters;
    }

    public static Retry getRetry(Entity entity) throws FalconException {
        switch (entity.getEntityType()) {
        case FEED:
            if (!RuntimeProperties.get()
                    .getProperty("feed.retry.allowed", "true")
                    .equalsIgnoreCase("true")) {
                return null;
            }
            Retry retry = new Retry();
            retry.setAttempts(Integer.parseInt(RuntimeProperties.get()
                    .getProperty("feed.retry.attempts", "3")));
            retry.setDelay(new Frequency(RuntimeProperties.get().getProperty(
                    "feed.retry.frequency", "minutes(5)")));
            retry.setPolicy(PolicyType.fromValue(RuntimeProperties.get()
                    .getProperty("feed.retry.policy", "exp-backoff")));
            return retry;
        case PROCESS:
            Process process = (Process) entity;
            return process.getRetry();
        default:
            throw new FalconException("Cannot create Retry for entity:" + entity.getName());
        }
    }

    //Staging path that stores scheduler configs like oozie coord/bundle xmls, parent workflow xml
    //Each entity update creates a new staging path
    //Base staging path is the base path for all staging dirs
    public static Path getBaseStagingPath(org.apache.falcon.entity.v0.cluster.Cluster cluster, Entity entity) {
        return new Path(ClusterHelper.getLocation(cluster, ClusterLocationType.STAGING).getPath(),
                "falcon/workflows/" + entity.getEntityType().name().toLowerCase() + "/" + entity.getName());
    }

    //Creates new staging path for entity schedule/update
    //Staging path containd md5 of the cluster view of the entity. This is required to check if update is required
    public static Path getNewStagingPath(org.apache.falcon.entity.v0.cluster.Cluster cluster, Entity entity)
        throws FalconException {
        Entity clusterView = getClusterView(entity, cluster.getName());
        return new Path(getBaseStagingPath(cluster, entity),
            md5(clusterView) + "_" + String.valueOf(System.currentTimeMillis()));
    }

    //Returns all staging paths for the entity
    public static FileStatus[] getAllStagingPaths(org.apache.falcon.entity.v0.cluster.Cluster cluster,
                                                  Entity entity) throws FalconException {
        Path basePath = getBaseStagingPath(cluster, entity);
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                ClusterHelper.getConfiguration(cluster));
        try {
            return fs.listStatus(basePath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return !path.getName().equals("logs");
                }
            });

        } catch (FileNotFoundException e) {
            LOG.info("Staging path " + basePath + " doesn't exist, entity is not scheduled");
            //Staging path doesn't exist if entity is not scheduled
        } catch (IOException e) {
            throw new FalconException(e);
        }
        return null;
    }

    public static LateProcess getLateProcess(Entity entity)
        throws FalconException {

        switch (entity.getEntityType()) {
        case FEED:
            if (!RuntimeProperties.get().getProperty("feed.late.allowed", "true").equalsIgnoreCase("true")) {
                return null;
            }

            //If late Arrival is not configured do not process further
            if (((Feed) entity).getLateArrival() == null){
                return null;
            }

            LateProcess lateProcess = new LateProcess();
            lateProcess.setDelay(new Frequency(RuntimeProperties.get().getProperty("feed.late.frequency", "hours(3)")));
            lateProcess.setPolicy(
                    PolicyType.fromValue(RuntimeProperties.get().getProperty("feed.late.policy", "exp-backoff")));
            LateInput lateInput = new LateInput();
            lateInput.setInput(entity.getName());
            //TODO - Assuming the late workflow is not used
            lateInput.setWorkflowPath("ignore.xml");
            lateProcess.getLateInputs().add(lateInput);
            return lateProcess;
        case PROCESS:
            Process process = (Process) entity;
            return process.getLateProcess();
        default:
            throw new FalconException("Cannot create Late Process for entity:" + entity.getName());
        }
    }

    public static Path getLogPath(org.apache.falcon.entity.v0.cluster.Cluster cluster, Entity entity) {
        return new Path(getBaseStagingPath(cluster, entity), "logs");
    }

    public static String fromUTCtoURIDate(String utc) throws FalconException {
        DateFormat utcFormat = new SimpleDateFormat(
                "yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        Date utcDate;
        try {
            utcDate = utcFormat.parse(utc);
        } catch (ParseException e) {
            throw new FalconException("Unable to parse utc date:", e);
        }
        DateFormat uriFormat = new SimpleDateFormat("yyyy'-'MM'-'dd'-'HH'-'mm");
        return uriFormat.format(utcDate);
    }

    public static boolean responsibleFor(String colo) {
        return DeploymentUtil.isEmbeddedMode() || (!DeploymentUtil.isPrism()
                && colo.equals(DeploymentUtil.getCurrentColo()));
    }

    public static Date getNextStartTime(Entity entity, Cluster cluster, Date effectiveTime) {
        switch(entity.getEntityType()) {
        case FEED:
            Feed feed = (Feed) entity;
            org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
            return getNextStartTime(feedCluster.getValidity().getStart(), feed.getFrequency(), feed.getTimezone(),
                effectiveTime);

        case PROCESS:
            Process process = (Process) entity;
            org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process,
                cluster.getName());
            return getNextStartTime(processCluster.getValidity().getStart(), process.getFrequency(),
                process.getTimezone(), effectiveTime);

        default:
        }

        throw new IllegalArgumentException("Unhandled type: " + entity.getEntityType());
    }

    public static boolean isTableStorageType(Cluster cluster, Entity entity) throws FalconException {
        return entity.getEntityType() == EntityType.PROCESS
            ? isTableStorageType(cluster, (Process) entity) : isTableStorageType(cluster, (Feed) entity);
    }

    public static boolean isTableStorageType(Cluster cluster, Feed feed) throws FalconException {
        Storage.TYPE storageType = FeedHelper.getStorageType(feed, cluster);
        return Storage.TYPE.TABLE == storageType;
    }

    public static boolean isTableStorageType(Cluster cluster, Process process) throws FalconException {
        Storage.TYPE storageType = ProcessHelper.getStorageType(cluster, process);
        return Storage.TYPE.TABLE == storageType;
    }

    public static List<String> getTags(Entity entity) {
        String rawTags = null;

        switch (entity.getEntityType()) {
        case PROCESS:
            rawTags = ((Process) entity).getTags();
            break;

        case FEED:
            rawTags = ((Feed) entity).getTags();
            break;

        case CLUSTER:
            rawTags = ((Cluster) entity).getTags();
            break;

        default:
            break;
        }

        List<String> tags = new ArrayList<String>();
        if (!StringUtils.isEmpty(rawTags)) {
            for(String tag : rawTags.split(",")) {
                tags.add(tag.trim());
            }
        }

        return tags;
    }

    public static List<String> getPipelines(Entity entity) {
        List<String> pipelines = new ArrayList<String>();

        if (entity.getEntityType().equals(EntityType.PROCESS)) {
            Process process = (Process) entity;
            String pipelineString = process.getPipelines();
            if (pipelineString != null) {
                for (String pipeline : pipelineString.split(",")) {
                    pipelines.add(pipeline.trim());
                }
            }
        } // else : Pipelines are only set for Process entities

        return pipelines;
    }

    public static Pair<Date, Date> getEntityStartEndDates(Entity entityObject) {
        Set<String> clusters = EntityUtil.getClustersDefined(entityObject);
        Pair<Date, String> clusterMinStartDate = null;
        Pair<Date, String> clusterMaxEndDate = null;
        for (String cluster : clusters) {
            if (clusterMinStartDate == null || clusterMinStartDate.first.after(getStartTime(entityObject, cluster))) {
                clusterMinStartDate = Pair.of(getStartTime(entityObject, cluster), cluster);
            }
            if (clusterMaxEndDate == null || clusterMaxEndDate.first.before(getEndTime(entityObject, cluster))) {
                clusterMaxEndDate = Pair.of(getEndTime(entityObject, cluster), cluster);
            }
        }
        return new Pair<Date, Date>(clusterMinStartDate.first, clusterMaxEndDate.first);
    }
}
