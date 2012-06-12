package org.apache.ivory.entity;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.WorkflowNameBuilder.WorkflowName;
import org.apache.ivory.entity.common.DateValidator;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.Frequency;
import org.apache.ivory.entity.v0.feed.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.LateInput;
import org.apache.ivory.entity.v0.process.LateProcess;
import org.apache.ivory.entity.v0.process.PolicyType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.entity.v0.process.Retry;
import org.apache.ivory.util.RuntimeProperties;

public class EntityUtil {

	private static final DateValidator DateValidator = new DateValidator();
	private static final long MINUTE_IN_MS = 60000L;
	private static final long HOUR_IN_MS = 3600000L;
	private static final long DAY_IN_MS = 86400000L;
	private static final long MONTH_IN_MS = 2592000000L;

	public static Entity getEntity(EntityType type, String entityName) throws IvoryException {
		ConfigurationStore configStore = ConfigurationStore.get();
		Entity entity = configStore.get(type, entityName);
		if (entity == null) {
			throw new EntityNotRegisteredException(entityName + " (" + type + ") not found");
		}
		return entity;        
	}

	public static Entity getEntity(String type, String entityName) throws IvoryException {
		EntityType entityType;
		try {
			entityType = EntityType.valueOf(type.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IvoryException("Invalid entity type: " + type, e);
		}
		return getEntity(entityType, entityName);
	}

	private static DateFormat getDateFormat() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return dateFormat;
	}

	public static TimeZone getTimeZone(String tzId) {
		if (tzId == null) {
			throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
		}
		TimeZone tz = TimeZone.getTimeZone(tzId);
		if (!tzId.equals("GMT") && tz.getID().equals("GMT")) {
			throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
		}
		return tz;
	}

	public static Date parseDateUTC(String s) throws IvoryException {
		try {
			return getDateFormat().parse(s);
		} catch (ParseException e) {
			throw new IvoryException(e);
		}
	}

	public static String formatDateUTC(Date d) {
		return (d != null) ? getDateFormat().format(d) : null;
	}

	public static boolean isValidUTCDate(String date) {
		return DateValidator.validate(date);
	}

	public static Date getEndTime(Entity entity, String cluster) throws IvoryException {
		if (entity.getEntityType() == EntityType.PROCESS) {
			return getEndTime((Process) entity, cluster);
		} else {
			return getEndTime((Feed) entity, cluster);
		}
	}

	public static Date getStartTime(Entity entity, String cluster) throws IvoryException {
		if (entity.getEntityType() == EntityType.PROCESS) {
			return getStartTime((Process) entity, cluster);
		} else {
			return getStartTime((Feed) entity, cluster);
		}
	}

	public static Date getEndTime(Process process, String cluster) throws IvoryException {
		org.apache.ivory.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
		return EntityUtil.parseDateUTC(processCluster.getValidity().getEnd());
	}

	public static Date getStartTime(Process process, String cluster) throws IvoryException {
		org.apache.ivory.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
		return EntityUtil.parseDateUTC(processCluster.getValidity().getStart());
	}

	public static Date getEndTime(Feed feed, String cluster) throws IvoryException {
		org.apache.ivory.entity.v0.feed.Cluster clusterDef = FeedHelper.getCluster(feed, cluster);
		return EntityUtil.parseDateUTC(clusterDef.getValidity().getEnd());
	}

	public static Date getStartTime(Feed feed, String cluster) throws IvoryException {
		org.apache.ivory.entity.v0.feed.Cluster clusterDef = FeedHelper.getCluster(feed, cluster);
		return EntityUtil.parseDateUTC(clusterDef.getValidity().getStart());
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
		org.apache.ivory.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
		processCluster.getValidity().setStart(EntityUtil.formatDateUTC(startDate));
	}

	public static void setParallel(Process process, int parallel) {
		process.setParallel(parallel);
	}

	public static void setEndTime(Process process, String cluster, Date endDate) {
		org.apache.ivory.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
		processCluster.getValidity().setEnd(EntityUtil.formatDateUTC(endDate));
	}

	public static int getParallel(Feed feed) {
		return 1;
	}

	public static void setStartDate(Feed feed, String cluster, Date startDate) {
		org.apache.ivory.entity.v0.feed.Cluster clusterDef = FeedHelper.getCluster(feed, cluster);
		clusterDef.getValidity().setStart(EntityUtil.formatDateUTC(startDate));
	}

	public static void setEndTime(Feed feed, String cluster, Date endDate) {
		org.apache.ivory.entity.v0.feed.Cluster clusterDef = FeedHelper.getCluster(feed, cluster);
		clusterDef.getValidity().setStart(EntityUtil.formatDateUTC(endDate));
	}

	public static void setParallel(Feed feed, int parallel) {
	}

	public static Date getNextStartTime(Date startTime, Frequency frequency, TimeZone timezone, Date now) {
		if (startTime.after(now))
			return startTime;

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

		if (count > 2) {
			startCal.add(frequency.getTimeUnit().getCalendarUnit(), ((count - 2) / frequency.getFrequency()) * frequency.getFrequency());
		}
		while (startCal.getTime().before(now)) {
			startCal.add(frequency.getTimeUnit().getCalendarUnit(), frequency.getFrequency());
		}
		return startCal.getTime();
	}

	public static int getInstanceSequence(Date startTime, Frequency frequency, TimeZone tz, Date instanceTime) {
		if (startTime.after(instanceTime))
			return -1;

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

		if (count > 2) {
			startCal.add(frequency.getTimeUnit().getCalendarUnit(), (count / frequency.getFrequency()) * frequency.getFrequency());
			count = (count / frequency.getFrequency());
		}
		while (startCal.getTime().before(instanceTime)) {
			startCal.add(frequency.getTimeUnit().getCalendarUnit(), frequency.getFrequency());
			count++;
		}
		return count + 1;
	}

	public static String getStagingPath(Entity entity) throws IvoryException {
		try {
			byte[] digest = DigestUtils.md5(entity.toComparableString());
			return "ivory/workflows/" + entity.getEntityType().name().toLowerCase() + "/" + entity.getName() + "/"
			+ new String(Hex.encodeHex(digest));
		} catch (Exception e) {
			throw new IvoryException(e);
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

	public static String getWorkflowNameSuffixes(String workflowName, Entity entity) {
		WorkflowNameBuilder<Entity> builder = new WorkflowNameBuilder<Entity>(
				entity);
		return builder.getWorkflowSuffixes(workflowName);
	}

	public static Tag getWorkflowNameTag(String workflowName, Entity entity) {
		WorkflowNameBuilder<Entity> builder = new WorkflowNameBuilder<Entity>(
				entity);
		return builder.getWorkflowTag(workflowName);
	}

	public static String[] getClustersDefined(Entity entity) {
		switch(entity.getEntityType()) {
		case CLUSTER:
			return new String[] { entity.getName() };

		case FEED:
			Feed feed = (Feed) entity;
			List<String> clusters = new ArrayList<String>();
			for(Cluster cluster:feed.getClusters().getClusters())
				clusters.add(cluster.getName());
			return clusters.toArray(new String[clusters.size()]);

		case PROCESS:
			Process process = (Process) entity;
			clusters = new ArrayList<String>();
			for(org.apache.ivory.entity.v0.process.Cluster cluster:process.getClusters().getClusters())
				clusters.add(cluster.getName());
			return clusters.toArray(new String[clusters.size()]);
		}  
		throw new IllegalArgumentException("Unhandled entity type: " + entity.getEntityType());
	}

	public static Path getStagingPath(
			org.apache.ivory.entity.v0.cluster.Cluster cluster, Entity entity)
					throws IvoryException {
		try {
			return new Path(ClusterHelper.getLocation(cluster, "staging"),
					EntityUtil.getStagingPath(entity));
		} catch (Exception e) {
			throw new IvoryException(e);
		}
	}

	public static Retry getRetry(Entity entity) throws IvoryException {
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
			throw new IvoryException("Cannot create Retry for entity:"+entity.getName());
		}
	}

	public static LateProcess getLateProcess(Entity entity)
			throws IvoryException {
		switch (entity.getEntityType()) {
		case FEED:
			if (!RuntimeProperties.get()
					.getProperty("feed.late.allowed", "true")
					.equalsIgnoreCase("true")) {
				return null;
			}
			LateProcess lateProcess = new LateProcess();
			lateProcess.setDelay(new Frequency(RuntimeProperties.get()
					.getProperty("feed.late.frequency", "hours(3)")));
			lateProcess.setPolicy(PolicyType.fromValue(RuntimeProperties.get()
					.getProperty("feed.retry.policy", "exp-backoff")));
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
			throw new IvoryException("Cannot create Late Process for entity:"+entity.getName());
		}
	}
}
