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

package org.apache.ivory.workflow.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.Frequency;
import org.apache.ivory.entity.v0.Frequency.TimeUnit;
import org.apache.ivory.entity.v0.SchemaHelper;
import org.apache.ivory.resource.APIResult;
import org.apache.ivory.resource.InstancesResult;
import org.apache.ivory.resource.InstancesResult.Instance;
import org.apache.ivory.resource.InstancesResult.WorkflowStatus;
import org.apache.ivory.update.UpdateHelper;
import org.apache.ivory.util.OozieUtils;
import org.apache.ivory.workflow.OozieWorkflowBuilder;
import org.apache.ivory.workflow.WorkflowBuilder;
import org.apache.log4j.Logger;
import org.apache.oozie.client.*;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.WorkflowJob.Status;

/**
 * Workflow engine which uses oozies APIs
 * 
 */
@SuppressWarnings("unchecked")
public class OozieWorkflowEngine implements WorkflowEngine {

	private static final Logger LOG = Logger
			.getLogger(OozieWorkflowEngine.class);

	public static final String ENGINE = "oozie";
	private static final BundleJob MISSING = new NullBundleJob();

	private static final WorkflowEngineActionListener listener = new OozieHouseKeepingService();

	private static List<Status> WF_KILL_PRECOND = Arrays.asList(Status.PREP,
			Status.RUNNING, Status.SUSPENDED, Status.FAILED);
	private static List<Status> WF_SUSPEND_PRECOND = Arrays
			.asList(Status.RUNNING);
	private static List<Status> WF_RESUME_PRECOND = Arrays
			.asList(Status.SUSPENDED);
	private static List<Status> WF_RERUN_PRECOND = Arrays.asList(Status.FAILED,
			Status.KILLED, Status.SUCCEEDED);

	private static List<Job.Status> BUNDLE_ACTIVE_STATUS = Arrays.asList(
			Job.Status.PREP, Job.Status.RUNNING, Job.Status.SUSPENDED,
			Job.Status.PREPSUSPENDED);
	private static List<Job.Status> BUNDLE_SUSPENDED_STATUS = Arrays.asList(
			Job.Status.PREPSUSPENDED, Job.Status.SUSPENDED);
	private static List<Job.Status> BUNDLE_RUNNING_STATUS = Arrays.asList(
			Job.Status.PREP, Job.Status.RUNNING);
	private static List<Job.Status> BUNDLE_KILLED_STATUS = Arrays
			.asList(Job.Status.KILLED);

	private static List<Job.Status> BUNDLE_KILL_PRECOND = BUNDLE_ACTIVE_STATUS;
	private static List<Job.Status> BUNDLE_SUSPEND_PRECOND = Arrays.asList(
			Job.Status.PREP, Job.Status.RUNNING, Job.Status.DONEWITHERROR);
	private static List<Job.Status> BUNDLE_RESUME_PRECOND = Arrays.asList(
			Job.Status.SUSPENDED, Job.Status.PREPSUSPENDED);
	private static final String IVORY_INSTANCE_ACTION_CLUSTERS = "ivory.instance.action.clusters";
	private static final String IVORY_INSTANCE_SOURCE_CLUSTERS = "ivory.instance.source.clusters";

	private static final String[] BUNDLE_UPDATEABLE_PROPS = new String[] {
			"parallel", "clusters.clusters[\\d+].validity.end" };

	@Override
	public void schedule(Entity entity) throws IvoryException {
		Map<String, BundleJob> bundleMap = findBundle(entity);
		List<String> schedClusters = new ArrayList<String>();
		for (String cluster : bundleMap.keySet()) {
			if (bundleMap.get(cluster) == MISSING)
				schedClusters.add(cluster);
		}

		if (!schedClusters.isEmpty()) {
			WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(
					ENGINE, entity);
			Map<String, Properties> newFlows = builder.newWorkflowSchedule(
					entity, schedClusters);
			for (String cluster : newFlows.keySet()) {
				LOG.info("Scheduling " + entity.toShortString()
						+ " on cluster " + cluster);
				scheduleEntity(cluster, newFlows.get(cluster), entity);
			}
		}
	}

	@Override
	public boolean isActive(Entity entity) throws IvoryException {
		return isBundleInState(entity, BundleStatus.ACTIVE);
	}

	@Override
	public boolean isSuspended(Entity entity) throws IvoryException {
		return isBundleInState(entity, BundleStatus.SUSPENDED);
	}

	private enum BundleStatus {
		ACTIVE, RUNNING, SUSPENDED
	}

	private boolean isBundleInState(Entity entity, BundleStatus status)
			throws IvoryException {
		Map<String, BundleJob> bundles = findLatestBundle(entity);
		for (BundleJob bundle : bundles.values()) {
			if (bundle == MISSING) // There is no active bundle
				return false;

			switch (status) {
			case ACTIVE:
				if (!BUNDLE_ACTIVE_STATUS.contains(bundle.getStatus()))
					return false;
				break;

			case RUNNING:
				if (!BUNDLE_RUNNING_STATUS.contains(bundle.getStatus()))
					return false;
				break;

			case SUSPENDED:
				if (!BUNDLE_SUSPENDED_STATUS.contains(bundle.getStatus()))
					return false;
				break;
			}
		}
		return true;
	}

	private Map<String, BundleJob> findBundle(Entity entity)
			throws IvoryException {
		String[] clusters = EntityUtil.getClustersDefinedInColos(entity);
		Map<String, BundleJob> jobMap = new HashMap<String, BundleJob>();
		for (String cluster : clusters) {
			jobMap.put(cluster, findBundle(entity, cluster));
		}
		return jobMap;
	}

	private BundleJob findBundle(Entity entity, String cluster)
			throws IvoryException {
		String stPath = EntityUtil.getStagingPath(entity);
		List<BundleJob> bundles = findBundles(entity, cluster);
		for (BundleJob job : bundles) {
			if (job.getAppPath().endsWith(stPath)) {
				return getBundleInfo(cluster, job.getId());
			}
		}
		return MISSING;
	}

	private List<BundleJob> findBundles(Entity entity, String cluster)
			throws IvoryException {
		try {
			OozieClient client = OozieClientFactory.get(cluster);
			List<BundleJob> jobs = client.getBundleJobsInfo(
					OozieClient.FILTER_NAME + "="
							+ EntityUtil.getWorkflowName(entity) + ";", 0, 256);
			if (jobs != null) {
				return jobs;
			}
			return new ArrayList<BundleJob>();
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	private Map<String, List<BundleJob>> findBundles(Entity entity)
			throws IvoryException {
		String[] clusters = EntityUtil.getClustersDefinedInColos(entity);
		Map<String, List<BundleJob>> jobMap = new HashMap<String, List<BundleJob>>();

		for (String cluster : clusters) {
			jobMap.put(cluster, findBundles(entity, cluster));
		}
		return jobMap;
	}

	// During update, a new bundle may not be created if next start time >= end
	// time
	// In this case, there will not be a bundle with the latest entity md5
	// So, pick last created bundle
	private Map<String, BundleJob> findLatestBundle(Entity entity)
			throws IvoryException {
		Map<String, List<BundleJob>> bundlesMap = findBundles(entity);
		Map<String, BundleJob> bundleMap = new HashMap<String, BundleJob>();
		for (String cluster : bundlesMap.keySet()) {
			Date latest = null;
			bundleMap.put(cluster, MISSING);
			for (BundleJob job : bundlesMap.get(cluster))
				if (latest == null || latest.before(job.getCreatedTime())) {
					bundleMap.put(cluster, job);
					latest = job.getCreatedTime();
				}
		}
		return bundleMap;
	}

	@Override
	public String suspend(Entity entity) throws IvoryException {
		return doBundleAction(entity, BundleAction.SUSPEND);
	}

	@Override
	public String resume(Entity entity) throws IvoryException {
		return doBundleAction(entity, BundleAction.RESUME);
	}

	@Override
	public String delete(Entity entity) throws IvoryException {
		return doBundleAction(entity, BundleAction.KILL);
	}

	private enum BundleAction {
		SUSPEND, RESUME, KILL
	}

	private String doBundleAction(Entity entity, BundleAction action)
			throws IvoryException {
		boolean success = true;
		Map<String, List<BundleJob>> jobsMap = findBundles(entity);
		for (String cluster : jobsMap.keySet()) {
			List<BundleJob> jobs = jobsMap.get(cluster);
			if (jobs.isEmpty()) {
				LOG.warn("No active job found for " + entity.getName());
				success = false;
				break;
			}

			beforeAction(entity, action, cluster);
			for (BundleJob job : jobs) {
				switch (action) {
				case SUSPEND:
					// not already suspended and preconditions are true
					if (!BUNDLE_SUSPENDED_STATUS.contains(job.getStatus())
							&& BUNDLE_SUSPEND_PRECOND.contains(job.getStatus())) {
						suspend(cluster, job.getId());
						success = true;
					}
					break;

				case RESUME:
					// not already running and preconditions are true
					if (!BUNDLE_RUNNING_STATUS.contains(job.getStatus())
							&& BUNDLE_RESUME_PRECOND.contains(job.getStatus())) {
						resume(cluster, job.getId());
						success = true;
					}
					break;

				case KILL:
					// not already killed and preconditions are true
					if (!BUNDLE_KILLED_STATUS.contains(job.getStatus())
							&& BUNDLE_KILL_PRECOND.contains(job.getStatus())) {
						kill(cluster, job.getId());
						success = true;
					}
					break;
				}
				afterAction(entity, action, cluster);
			}
		}
		return success ? "SUCCESS" : "FAILED";
	}

	private void beforeAction(Entity entity, BundleAction action, String cluster)
			throws IvoryException {
		switch (action) {
		case SUSPEND:
			listener.beforeSuspend(entity, cluster);
			break;

		case RESUME:
			listener.beforeResume(entity, cluster);
			break;

		case KILL:
			listener.beforeDelete(entity, cluster);
			break;
		}
	}

	private void afterAction(Entity entity, BundleAction action, String cluster)
			throws IvoryException {
		switch (action) {
		case SUSPEND:
			listener.afterSuspend(entity, cluster);
			break;

		case RESUME:
			listener.afterResume(entity, cluster);
			break;

		case KILL:
			listener.afterDelete(entity, cluster);
			break;
		}
	}

	@Override
	public InstancesResult getRunningInstances(Entity entity)
			throws IvoryException {
		try {
			WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(
					ENGINE, entity);
			String[] clusters = EntityUtil.getClustersDefinedInColos(entity);
			List<Instance> runInstances = new ArrayList<Instance>();
			String[] wfNames = builder.getWorkflowNames(entity);
			List<String> coordNames = new ArrayList<String>();
			for (String wfName : wfNames) {
				if (EntityUtil.getWorkflowName(Tag.RETENTION, entity)
						.toString().equals(wfName))
					continue;
				coordNames.add(wfName);
			}

			for (String cluster : clusters) {
				OozieClient client = OozieClientFactory.get(cluster);
				List<WorkflowJob> wfs = getRunningWorkflows(cluster, coordNames);
				if (wfs != null) {
					for (WorkflowJob job : wfs) {
						WorkflowJob wf = client.getJobInfo(job.getId());
						if (StringUtils.isEmpty(wf.getParentId()))
							continue;

						CoordinatorAction action = client.getCoordActionInfo(wf
								.getParentId());
						String nominalTimeStr = SchemaHelper
								.formatDateUTC(action.getNominalTime());
						Instance instance = new Instance(cluster,
								nominalTimeStr, WorkflowStatus.RUNNING);
						instance.startTime = wf.getStartTime();
						if (entity.getEntityType() == EntityType.FEED) {
							instance.sourceCluster = getSourceCluster(cluster,
									action, entity);
						}
						runInstances.add(instance);
					}
				}
			}
			return new InstancesResult("Running Instances",
					runInstances.toArray(new Instance[runInstances.size()]));

		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	@Override
	public InstancesResult killInstances(Entity entity, Date start, Date end,
			Properties props) throws IvoryException {
		return doJobAction(JobAction.KILL, entity, start, end, props);
	}

	@Override
	public InstancesResult reRunInstances(Entity entity, Date start, Date end,
			Properties props) throws IvoryException {
		return doJobAction(JobAction.RERUN, entity, start, end, props);
	}

	@Override
	public InstancesResult suspendInstances(Entity entity, Date start,
			Date end, Properties props) throws IvoryException {
		return doJobAction(JobAction.SUSPEND, entity, start, end, props);
	}

	@Override
	public InstancesResult resumeInstances(Entity entity, Date start, Date end,
			Properties props) throws IvoryException {
		return doJobAction(JobAction.RESUME, entity, start, end, props);
	}

	@Override
	public InstancesResult getStatus(Entity entity, Date start, Date end)
			throws IvoryException {
		return doJobAction(JobAction.STATUS, entity, start, end, null);
	}

	private static enum JobAction {
		KILL, SUSPEND, RESUME, RERUN, STATUS
	}

	private WorkflowJob getWorkflowInfo(String cluster, String wfId)
			throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			return client.getJobInfo(wfId);
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	private InstancesResult doJobAction(JobAction action, Entity entity,
			Date start, Date end, Properties props) throws IvoryException {

		Map<String, List<CoordinatorAction>> actionsMap = getCoordActions(
				entity, start, end);
		List<String> clusterList = getIncludedClusters(props,
				IVORY_INSTANCE_ACTION_CLUSTERS);
		List<String> sourceClusterList = getIncludedClusters(props,
				IVORY_INSTANCE_SOURCE_CLUSTERS);
		APIResult.Status overallStatus = APIResult.Status.SUCCEEDED;
		int instanceCount = 0;

		List<Instance> instances = new ArrayList<Instance>();
		for (String cluster : actionsMap.keySet()) {
			if (clusterList.size() != 0 && !clusterList.contains(cluster))
				continue;

			List<CoordinatorAction> actions = actionsMap.get(cluster);
			String sourceCluster = null;
			for (CoordinatorAction coordinatorAction : actions) {
				if (entity.getEntityType() == EntityType.FEED) {
					sourceCluster = getSourceCluster(cluster,
							coordinatorAction, entity);
					if (sourceClusterList.size() != 0
							&& !sourceClusterList.contains(sourceCluster))
						continue;
				}
				String status = mapActionStatus(coordinatorAction.getStatus());
				WorkflowJob jobInfo = null;
				if (coordinatorAction.getExternalId() != null) {
					jobInfo = getWorkflowInfo(cluster,
							coordinatorAction.getExternalId());
				}
				instanceCount++;
				if (jobInfo != null) {
					status = mapWorkflowStatus(jobInfo.getStatus());
					try {
						status = performAction(action, props, cluster, status,
								jobInfo);
					} catch (IvoryException e) {
						LOG.warn("Unable to perform action " + action
								+ " on cluster ", e);
						status = WorkflowStatus.ERROR.name();
						overallStatus = APIResult.Status.PARTIAL;
					}
				}
				if (action != OozieWorkflowEngine.JobAction.STATUS
						&& coordinatorAction.getExternalId() != null) {
					jobInfo = getWorkflowInfo(cluster,
							coordinatorAction.getExternalId());
				}

				String nominalTimeStr = SchemaHelper
						.formatDateUTC(coordinatorAction.getNominalTime());
				InstancesResult.Instance instance = new InstancesResult.Instance(
						cluster, nominalTimeStr, WorkflowStatus.valueOf(status));
				if (jobInfo != null) {
					instance.startTime = jobInfo.getStartTime();
					instance.endTime = jobInfo.getEndTime();
					instance.logFile = jobInfo.getConsoleUrl();
					instance.sourceCluster = sourceCluster;
					instance.details = coordinatorAction
							.getMissingDependencies();
				}
				instances.add(instance);
			}
		}
		if (instanceCount < 2 && overallStatus == APIResult.Status.PARTIAL) {
			overallStatus = APIResult.Status.FAILED;
		}
		InstancesResult instancesResult = new InstancesResult(overallStatus,
				action.name());
		instancesResult.setInstances(instances.toArray(new Instance[instances
				.size()]));
		return instancesResult;
	}

	private String performAction(JobAction action, Properties props,
			String cluster, String status, WorkflowJob jobInfo)
			throws IvoryException {
		switch (action) {
		case KILL:
			if (!WF_KILL_PRECOND.contains(jobInfo.getStatus()))
				break;

			kill(cluster, jobInfo.getId());
			status = Status.KILLED.name();
			break;

		case SUSPEND:
			if (!WF_SUSPEND_PRECOND.contains(jobInfo.getStatus()))
				break;

			suspend(cluster, jobInfo.getId());
			status = Status.SUSPENDED.name();
			break;

		case RESUME:
			if (!WF_RESUME_PRECOND.contains(jobInfo.getStatus()))
				break;

			resume(cluster, jobInfo.getId());
			status = Status.RUNNING.name();
			break;

		case RERUN:
			if (!WF_RERUN_PRECOND.contains(jobInfo.getStatus()))
				break;

			reRun(cluster, jobInfo.getId(), props);
			status = Status.RUNNING.name();
			break;

		case STATUS:
			break;
		}
		return status;
	}

	private String getSourceCluster(String cluster,
			CoordinatorAction coordinatorAction, Entity entity)
			throws IvoryException {

		OozieClient client = OozieClientFactory.get(cluster);
		CoordinatorJob coordJob;
		try {
			coordJob = client.getCoordJobInfo(coordinatorAction.getJobId());
		} catch (OozieClientException e) {
			throw new IvoryException("Unable to get oozie job id:" + e);
		}
		return EntityUtil.getWorkflowNameSuffix(coordJob.getAppName(), entity);
	}

	private List<String> getIncludedClusters(Properties props,
			String clustersType) {
		String clusters = props == null ? "" : props.getProperty(clustersType,
				"");
		List<String> clusterList = new ArrayList<String>();
		for (String cluster : clusters.split(",")) {
			if (StringUtils.isNotEmpty(cluster)) {
				clusterList.add(cluster.trim());
			}
		}
		return clusterList;
	}

	private String mapActionStatus(CoordinatorAction.Status status) {
		if (status == CoordinatorAction.Status.READY
				|| status == CoordinatorAction.Status.WAITING
				|| status == CoordinatorAction.Status.TIMEDOUT
				|| status == CoordinatorAction.Status.SUBMITTED) {
			return InstancesResult.WorkflowStatus.WAITING.name();
		} else if (status == CoordinatorAction.Status.DISCARDED) {
			return InstancesResult.WorkflowStatus.KILLED.name();
		} else {
			return status.name();
		}
	}

	private String mapWorkflowStatus(WorkflowJob.Status status) {
		if (status == WorkflowJob.Status.PREP) {
			return InstancesResult.WorkflowStatus.RUNNING.name();
		} else {
			return status.name();
		}
	}

	protected Map<String, List<CoordinatorAction>> getCoordActions(
			Entity entity, Date start, Date end) throws IvoryException {
		Map<String, List<BundleJob>> bundlesMap = findBundles(entity);
		Map<String, List<CoordinatorAction>> actionsMap = new HashMap<String, List<CoordinatorAction>>();

		for (String cluster : bundlesMap.keySet()) {
			List<BundleJob> bundles = bundlesMap.get(cluster);
			OozieClient client = OozieClientFactory.get(cluster);
			List<CoordinatorJob> applicableCoords = getApplicableCoords(entity,
					client, start, end, bundles);
			List<CoordinatorAction> actions = new ArrayList<CoordinatorAction>();

			for (CoordinatorJob coord : applicableCoords) {
				Frequency freq = createFrequency(coord.getFrequency(),
						coord.getTimeUnit());
				TimeZone tz = EntityUtil.getTimeZone(coord.getTimeZone());
				Date iterStart = EntityUtil.getNextStartTime(
						coord.getStartTime(), freq, tz, start);
				final Date iterEnd = (coord.getEndTime().before(end) ? coord
						.getEndTime() : end);
				while (!iterStart.after(iterEnd)) {
					int sequence = EntityUtil.getInstanceSequence(
							coord.getStartTime(), freq, tz, iterStart);
					String actionId = coord.getId() + "@" + sequence;
					CoordinatorAction coordActionInfo = null;
					try {
						coordActionInfo = client.getCoordActionInfo(actionId);
					} catch (OozieClientException e) {
						LOG.debug("Unable to get action for " + actionId + " "
								+ e.getMessage());
					}
					if (coordActionInfo != null) {
						actions.add(coordActionInfo);
					}
					Calendar startCal = Calendar.getInstance(EntityUtil
							.getTimeZone(coord.getTimeZone()));
					startCal.setTime(iterStart);
					startCal.add(freq.getTimeUnit().getCalendarUnit(),
							coord.getFrequency());
					iterStart = startCal.getTime();
				}
			}
			actionsMap.put(cluster, actions);
		}
		return actionsMap;
	}

	private Frequency createFrequency(int frequency, Timeunit timeUnit) {
		return new Frequency(frequency, OozieTimeUnit.valueOf(timeUnit.name())
				.getIvoryTimeUnit());
	}

	private enum OozieTimeUnit {
		MINUTE(TimeUnit.minutes), HOUR(TimeUnit.hours), DAY(TimeUnit.days), WEEK(
				null), MONTH(TimeUnit.months), END_OF_DAY(null), END_OF_MONTH(
				null), NONE(null);

		private TimeUnit ivoryTimeUnit;

		private OozieTimeUnit(TimeUnit ivoryTimeUnit) {
			this.ivoryTimeUnit = ivoryTimeUnit;
		}

		public TimeUnit getIvoryTimeUnit() {
			if (ivoryTimeUnit == null)
				throw new IllegalStateException("Invalid coord frequency: "
						+ name());
			return ivoryTimeUnit;
		}
	}

	private List<CoordinatorJob> getApplicableCoords(Entity entity,
			OozieClient client, Date start, Date end, List<BundleJob> bundles)
			throws IvoryException {
		List<CoordinatorJob> applicableCoords = new ArrayList<CoordinatorJob>();
		try {
			for (BundleJob bundle : bundles) {
				List<CoordinatorJob> coords = client.getBundleJobInfo(
						bundle.getId()).getCoordinators();
				for (CoordinatorJob coord : coords) {
					String coordName = EntityUtil.getWorkflowName(
							Tag.RETENTION, entity).toString();
					if (coordName.equals(coord.getAppName()))
						continue;
					// if end time is before coord-start time or start time is
					// after coord-end time ignore.
					if (!(end.compareTo(coord.getStartTime()) <= 0 || start
							.compareTo(coord.getEndTime()) >= 0)) {
						applicableCoords.add(coord);
					}
				}
			}
			sortCoordsByStartTime(applicableCoords);
			return applicableCoords;
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	protected void sortCoordsByStartTime(List<CoordinatorJob> consideredCoords) {
		Collections.sort(consideredCoords, new Comparator<CoordinatorJob>() {
			@Override
			public int compare(CoordinatorJob left, CoordinatorJob right) {
				Date leftStart = left.getStartTime();
				Date rightStart = right.getStartTime();
				return leftStart.compareTo(rightStart);
			}
		});
	}

	private boolean canUpdateBundle(Entity oldEntity, Entity newEntity)
			throws IvoryException {
		return EntityUtil.equals(oldEntity, newEntity, BUNDLE_UPDATEABLE_PROPS);
	}

	@Override
	public void update(Entity oldEntity, Entity newEntity)
			throws IvoryException {
		if (!UpdateHelper.shouldUpdate(oldEntity, newEntity))
			return;

		Map<String, BundleJob> bundleMap = findBundle(oldEntity);

		LOG.info("Updating entity through Workflow Engine"
				+ newEntity.toShortString());
		for (Map.Entry<String, BundleJob> entry : bundleMap.entrySet()) {
			if (entry.getValue() == MISSING)
				continue;

			String cluster = entry.getKey();
			BundleJob bundle = entry.getValue();
			Date newEndTime = EntityUtil.getEndTime(newEntity, cluster);
			if (newEndTime.before(now())) {
				throw new IvoryException(
						"New end time for "
								+ newEntity.getName()
								+ " is past current time. Entity can't be updated. Use remove and add");
			}

			LOG.debug("Updating for cluster : " + cluster + ", bundle: "
					+ bundle.getId());

			if (canUpdateBundle(oldEntity, newEntity)) {
				// only concurrency and endtime are changed. So, change coords
				LOG.info("Change operation is adequate! : " + cluster
						+ ", bundle: " + bundle.getId());
				updateCoords(cluster, bundle.getId(),
						EntityUtil.getParallel(newEntity),
						EntityUtil.getEndTime(newEntity, cluster));
				return;
			}

			LOG.debug("Going to update ! : " + newEntity.toShortString()
					+ cluster + ", bundle: " + bundle.getId());
			updateInternal(oldEntity, newEntity, cluster, bundle, false);
			LOG.info("Entity update complete : " + newEntity.toShortString()
					+ cluster + ", bundle: " + bundle.getId());
		}

		Set<Entity> affectedEntities = EntityGraph.get().getDependents(
				oldEntity);
		for (Entity entity : affectedEntities) {
			if (entity.getEntityType() != EntityType.PROCESS)
				continue;
			LOG.info("Dependent entities need to be updated "
					+ entity.toShortString());
			if (!UpdateHelper.shouldUpdate(oldEntity, newEntity, entity))
				continue;
			Map<String, BundleJob> processBundles = findBundle(entity);
			for (Map.Entry<String, BundleJob> processBundle : processBundles
					.entrySet()) {
				if (processBundle.getValue() == MISSING)
					continue;
				BundleJob feedBundle = findBundle(newEntity,
						processBundle.getKey());
				if (feedBundle == MISSING
						&& bundleMap.get(processBundle.getKey()) != MISSING) {
					throw new IllegalStateException(
							"Unable to find feed bundle in "
									+ processBundle.getKey() + " for entity "
									+ newEntity.getName());
				}
				LOG.info("Triggering update for " + processBundle.getKey()
						+ ", " + processBundle.getValue().getId());
				boolean processCreated = feedBundle.getCreatedTime().before(
						processBundle.getValue().getCreatedTime());
				updateInternal(entity, entity, processBundle.getKey(),
						processBundle.getValue(), processCreated);
				LOG.info("Entity update complete : " + entity.toShortString()
						+ processBundle.getKey() + ", bundle: "
						+ processBundle.getValue().getId());
			}
		}
		LOG.info("Entity update and all dependent entities updated: "
				+ oldEntity.toShortString());
	}

	private Date now() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	private Date offsetTime(Date date, int minute) {
		return new Date(date.getTime() + minute * 60 * 1000);
	}

	private Date getCoordLastActionTime(CoordinatorJob coord) {
		if (coord.getNextMaterializedTime() != null) {
			Calendar cal = Calendar.getInstance(EntityUtil.getTimeZone(coord
					.getTimeZone()));
			cal.setTime(coord.getLastActionTime());
			Frequency freq = createFrequency(coord.getFrequency(),
					coord.getTimeUnit());
			cal.add(freq.getTimeUnit().getCalendarUnit(), -1);
			return cal.getTime();
		}
		return null;
	}

	private void updateCoords(String cluster, String bundleId, int concurrency,
			Date endTime) throws IvoryException {
		if (endTime.compareTo(now()) <= 0)
			throw new IvoryException("End time "
					+ SchemaHelper.formatDateUTC(endTime)
					+ " can't be in the past");

		BundleJob bundle = getBundleInfo(cluster, bundleId);
		// change coords
		for (CoordinatorJob coord : bundle.getCoordinators()) {
			LOG.debug("Updating endtime of coord " + coord.getId() + " to "
					+ SchemaHelper.formatDateUTC(endTime) + " on cluster "
					+ cluster);
			Date lastActionTime = getCoordLastActionTime(coord);
			if (lastActionTime == null) { // nothing is materialized
				LOG.info("Nothing is materialized for this coord: "
						+ coord.getId());
				if (endTime.compareTo(coord.getStartTime()) <= 0) {
					LOG.info("Setting end time to START TIME "
							+ SchemaHelper.formatDateUTC(coord.getStartTime()));
					change(cluster, coord.getId(), concurrency,
							coord.getStartTime(), null);
				} else {
					LOG.info("Setting end time to START TIME "
							+ SchemaHelper.formatDateUTC(endTime));
					change(cluster, coord.getId(), concurrency, endTime, null);
				}
			} else {
				LOG.info("Actions have materialized for this coord: "
						+ coord.getId() + ", last action "
						+ SchemaHelper.formatDateUTC(lastActionTime));
				if (!endTime.after(lastActionTime)) {
					Date pauseTime = offsetTime(endTime, -1);
					// set pause time which deletes future actions
					LOG.info("Setting pause time on coord : " + coord.getId()
							+ " to " + SchemaHelper.formatDateUTC(pauseTime));
					change(cluster, coord.getId(), concurrency, null,
							SchemaHelper.formatDateUTC(pauseTime));
				}
				change(cluster, coord.getId(), concurrency, endTime, "");
			}
		}
	}

	private void suspend(String cluster, BundleJob bundle)
			throws IvoryException {
		for (CoordinatorJob coord : bundle.getCoordinators()) {
			suspend(cluster, coord.getId());
		}
	}

	private void resume(String cluster, BundleJob bundle) throws IvoryException {
		for (CoordinatorJob coord : bundle.getCoordinators()) {
			resume(cluster, coord.getId());
		}
	}

	private void updateInternal(Entity oldEntity, Entity newEntity,
			String cluster, BundleJob bundle, boolean alreadyCreated)
			throws IvoryException {
		OozieWorkflowBuilder<Entity> builder = (OozieWorkflowBuilder<Entity>) WorkflowBuilder
				.getBuilder(ENGINE, oldEntity);

		// Change end time of coords and schedule new bundle
		Job.Status oldBundleStatus = bundle.getStatus();
		suspend(cluster, bundle);

		BundleJob newBundle = findBundle(newEntity, cluster);
		Date endTime;
		if (newBundle == MISSING || !alreadyCreated) { // new entity is not
														// scheduled yet
			LOG.info("New bundle hasn't been created yet. So will create one");
			endTime = offsetTime(now(), 3);
			Date newStartTime = builder.getNextStartTime(newEntity, cluster,
					endTime);
			scheduleForUpdate(newEntity, cluster, newStartTime);
			LOG.info("New bundle scheduled successfully "
					+ SchemaHelper.formatDateUTC(newStartTime));
		} else {
			LOG.info("New bundle has already been created. Bundle Id: "
					+ newBundle.getId() + ", Start: "
					+ SchemaHelper.formatDateUTC(newBundle.getStartTime())
					+ ", End: " + newBundle.getEndTime());
			endTime = getMinStartTime(newBundle);
			LOG.info("Will set old coord end time to "
					+ SchemaHelper.formatDateUTC(endTime));
		}
		if (endTime != null)
			updateCoords(cluster, bundle.getId(),
					EntityUtil.getParallel(oldEntity), endTime);

		if (oldBundleStatus != Job.Status.SUSPENDED
				&& oldBundleStatus != Job.Status.PREPSUSPENDED) {
			resume(cluster, bundle);
		}
	}

	private void scheduleForUpdate(Entity entity, String cluster, Date startDate)
			throws IvoryException {
		WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(ENGINE,
				entity);
		Properties bundleProps = builder.newWorkflowSchedule(entity, startDate,
				cluster);
		LOG.info("Scheduling " + entity.toShortString() + " on cluster "
				+ cluster + " with props " + bundleProps);
		if (bundleProps != null) {
			scheduleEntity(cluster, bundleProps, entity);
		} else {
			LOG.info("No new workflow to be scheduled for this "
					+ entity.toShortString());
		}
	}

	private Date getMinStartTime(BundleJob bundle) {
		Date startTime = null;
		if (bundle.getCoordinators() != null)
			for (CoordinatorJob coord : bundle.getCoordinators())
				if (startTime == null || startTime.after(coord.getStartTime()))
					startTime = coord.getStartTime();
		return startTime;
	}

	private BundleJob getBundleInfo(String cluster, String bundleId)
			throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			return client.getBundleJobInfo(bundleId);
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	private List<WorkflowJob> getRunningWorkflows(String cluster,
			List<String> wfNames) throws IvoryException {
		StringBuilder filter = new StringBuilder();
		filter.append(OozieClient.FILTER_STATUS).append('=')
				.append(Job.Status.RUNNING.name());
		for (String wfName : wfNames)
			filter.append(';').append(OozieClient.FILTER_NAME).append('=')
					.append(wfName);

		OozieClient client = OozieClientFactory.get(cluster);
		try {
			return client.getJobsInfo(filter.toString(), 1, 1000);
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	@Override
	public void reRun(String cluster, String jobId, Properties props)
			throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			WorkflowJob jobInfo = client.getJobInfo(jobId);
			Properties jobprops = OozieUtils.toProperties(jobInfo.getConf());
			if (props == null || props.isEmpty())
				jobprops.put(OozieClient.RERUN_FAIL_NODES, "false");
			else
				for (Entry<Object, Object> entry : props.entrySet()) {
					jobprops.put(entry.getKey(), entry.getValue());
				}
			jobprops.remove(OozieClient.COORDINATOR_APP_PATH);
			jobprops.remove(OozieClient.BUNDLE_APP_PATH);
			client.reRun(jobId, jobprops);
			assertStatus(cluster, jobId, WorkflowJob.Status.RUNNING);
			LOG.info("Rerun job " + jobId + " on cluster " + cluster);
		} catch (Exception e) {
			LOG.error("Unable to rerun workflows", e);
			throw new IvoryException(e);
		}
	}

	private void assertStatus(String cluster, String jobId, Status... statuses)
			throws IvoryException {
		String actualStatus = getWorkflowStatus(cluster, jobId);
		for (int counter = 0; counter < 3; counter++) {
			if (!statusEquals(actualStatus, statuses)) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException ignore) {
				}
			} else {
				return;
			}
			actualStatus = getWorkflowStatus(cluster, jobId);
		}
		throw new IvoryException("For Job" + jobId + ", actual statuses: "
				+ actualStatus + ", expected statuses: "
				+ Arrays.toString(statuses));
	}

	private boolean statusEquals(String left, Status... right) {
		for (Status rightElement : right) {
			if (left.equals(rightElement.name()))
				return true;
		}
		return false;
	}

	@Override
	public String getWorkflowStatus(String cluster, String jobId)
			throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			WorkflowJob jobInfo = client.getJobInfo(jobId);
			Status status = jobInfo.getStatus();
			return status.name();
		} catch (Exception e) {
			LOG.error("Unable to get status of workflows", e);
			throw new IvoryException(e);
		}
	}

	private String scheduleEntity(String cluster, Properties props,
			Entity entity) throws IvoryException {
		listener.beforeSchedule(entity, cluster);
		String jobId = run(cluster, props);
		listener.afterSchedule(entity, cluster);
		return jobId;
	}

	private String run(String cluster, Properties props) throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			String jobId = client.run(props);
			LOG.info("Submitted " + jobId + " on cluster " + cluster
					+ " with properties : " + props);
			return jobId;
		} catch (OozieClientException e) {
			LOG.error("Unable to schedule workflows", e);
			throw new IvoryException("Unable to schedule workflows", e);
		}
	}

	private void suspend(String cluster, String jobId) throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			client.suspend(jobId);
			assertStatus(cluster, jobId, Status.SUSPENDED, Status.SUCCEEDED,
					Status.FAILED, Status.KILLED);
			LOG.info("Suspended job " + jobId + " on cluster " + cluster);
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	private void resume(String cluster, String jobId) throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			client.resume(jobId);
			assertStatus(cluster, jobId, Status.RUNNING, Status.SUCCEEDED,
					Status.FAILED, Status.KILLED);
			LOG.info("Resumed job " + jobId + " on cluster " + cluster);
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	private void kill(String cluster, String jobId) throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			client.kill(jobId);
			assertStatus(cluster, jobId, Status.KILLED, Status.SUCCEEDED,
					Status.FAILED);
			LOG.info("Killed job " + jobId + " on cluster " + cluster);
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	private void change(String cluster, String jobId, String changeValue)
			throws IvoryException {
		try {
			OozieClient client = OozieClientFactory.get(cluster);
			client.change(jobId, changeValue);
			LOG.info("Changed bundle/coord " + jobId + ": " + changeValue
					+ " on cluster " + cluster);
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	private void change(String cluster, String id, int concurrency,
			Date endTime, String pauseTime) throws IvoryException {
		StringBuilder changeValue = new StringBuilder();
		changeValue.append(OozieClient.CHANGE_VALUE_CONCURRENCY).append("=")
				.append(concurrency).append(";");
		if (endTime != null) {
			String endTimeStr = SchemaHelper.formatDateUTC(endTime);
			changeValue.append(OozieClient.CHANGE_VALUE_ENDTIME).append("=")
					.append(endTimeStr).append(";");
		}
		if (pauseTime != null)
			changeValue.append(OozieClient.CHANGE_VALUE_PAUSETIME).append("=")
					.append(pauseTime);

		String changeValueStr = changeValue.toString();
		if (changeValue.toString().endsWith(";"))
			changeValueStr = changeValue.substring(0,
					changeValueStr.length() - 1);

		change(cluster, id, changeValueStr);

		// assert that its really changed
		try {
			OozieClient client = OozieClientFactory.get(cluster);
			CoordinatorJob coord = client.getCoordJobInfo(id);
			for (int counter = 0; counter < 3; counter++) {
				Date intendedPauseTime = (StringUtils.isEmpty(pauseTime) ? null
						: SchemaHelper.parseDateUTC(pauseTime));
				if (coord.getConcurrency() != concurrency
						|| (endTime != null && !coord.getEndTime().equals(
								endTime))
						|| (intendedPauseTime != null && !intendedPauseTime
								.equals(coord.getPauseTime()))) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException ignore) {
					}
				} else {
					return;
				}
				coord = client.getCoordJobInfo(id);
			}
			LOG.error("Failed to change coordinator. Current value "
					+ coord.getConcurrency() + ", "
					+ SchemaHelper.formatDateUTC(coord.getEndTime()) + ", "
					+ SchemaHelper.formatDateUTC(coord.getPauseTime()));
			throw new IvoryException("Failed to change coordinator " + id
					+ " with change value " + changeValueStr);
		} catch (OozieClientException e) {
			throw new IvoryException(e);
		}
	}

	@Override
	public String getWorkflowProperty(String cluster, String jobId,
			String property) throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		try {
			WorkflowJob jobInfo = client.getJobInfo(jobId);
			String conf = jobInfo.getConf();
			Properties props = OozieUtils.toProperties(conf);
			return props.getProperty(property);
		} catch (Exception e) {
			throw new IvoryException(e);
		}
	}

	@Override
	public InstancesResult getJobDetails(String cluster, String jobId)
			throws IvoryException {
		OozieClient client = OozieClientFactory.get(cluster);
		Instance[] instances = new Instance[1];
		Instance instance = new Instance();
		try {
			WorkflowJob jobInfo = client.getJobInfo(jobId);
			instance.startTime = jobInfo.getStartTime();
			if (jobInfo.getStatus() == Status.RUNNING) {
				instance.endTime = new Date();
			} else {
				instance.endTime = jobInfo.getEndTime();
			}
			instance.cluster = cluster;
			instances[0] = instance;
			return new InstancesResult("Instance for workflow id:" + jobId,
					instances);
		} catch (Exception e) {
			throw new IvoryException(e);
		}

	}

}
