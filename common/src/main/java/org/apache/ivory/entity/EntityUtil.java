package org.apache.ivory.entity;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.WorkflowNameBuilder.WorkflowName;
import org.apache.ivory.entity.common.DateValidator;
import org.apache.ivory.entity.common.TimeUnit;
import org.apache.ivory.entity.parser.Frequency;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;

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
        return EntityUtil.parseDateUTC(process.getValidity().getEnd());
    }

    public static Date getStartTime(Process process, String cluster) throws IvoryException {
        return EntityUtil.parseDateUTC(process.getValidity().getStart());
    }

    public static Date getEndTime(Feed feed, String cluster) throws IvoryException {
        org.apache.ivory.entity.v0.feed.Cluster clusterDef = feed.getCluster(cluster);
        return EntityUtil.parseDateUTC(clusterDef.getValidity().getEnd());
    }

    public static Date getStartTime(Feed feed, String cluster) throws IvoryException {
        org.apache.ivory.entity.v0.feed.Cluster clusterDef = feed.getCluster(cluster);
        return EntityUtil.parseDateUTC(clusterDef.getValidity().getStart());
    }

    public static int getConcurrency(Entity entity) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            return getConcurrency((Process) entity);
        } else {
            return getConcurrency((Feed) entity);
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

    public static void setConcurrency(Entity entity, int concurrency) {
        if (entity.getEntityType() == EntityType.PROCESS) {
            setConcurrency((Process) entity, concurrency);
        } else {
            setConcurrency((Feed) entity, concurrency);
        }
    }

    public static int getConcurrency(Process process) {
        return process.getConcurrency();
    }

    public static void setStartDate(Process process, String cluster, Date startDate) {
        process.getValidity().setStart(EntityUtil.formatDateUTC(startDate));
    }

    public static void setConcurrency(Process process, int concurrency) {
        process.setConcurrency(concurrency);
    }

    public static void setEndTime(Process process, String cluster, Date endDate) {
        process.getValidity().setEnd(EntityUtil.formatDateUTC(endDate));
    }

    public static int getConcurrency(Feed feed) {
        return 1;
    }

    public static void setStartDate(Feed feed, String cluster, Date startDate) {
        org.apache.ivory.entity.v0.feed.Cluster clusterDef = feed.getCluster(cluster);
        clusterDef.getValidity().setStart(EntityUtil.formatDateUTC(startDate));
    }

    public static void setEndTime(Feed feed, String cluster, Date endDate) {
        org.apache.ivory.entity.v0.feed.Cluster clusterDef = feed.getCluster(cluster);
        clusterDef.getValidity().setStart(EntityUtil.formatDateUTC(endDate));
    }

    public static void setConcurrency(Feed feed, int concurrency) {
    }

    public static Date getNextStartTime(Date startTime, Frequency frequency, int periodicity, String timezone, Date now) {
        return getNextStartTime(startTime, frequency.getTimeUnit(), periodicity, timezone, now);
    }

    public static Date getNextStartTime(Date startTime, TimeUnit timeUnit, int periodicity, String timezone, Date now) {

        if (startTime.after(now))
            return startTime;

        Calendar startCal = Calendar.getInstance(EntityUtil.getTimeZone(timezone));
        startCal.setTime(startTime);

        int count = 0;
        switch (timeUnit) {
            case MONTH:
                count = (int) ((now.getTime() - startTime.getTime()) / MONTH_IN_MS);
                break;
            case DAY:
                count = (int) ((now.getTime() - startTime.getTime()) / DAY_IN_MS);
                break;
            case HOUR:
                count = (int) ((now.getTime() - startTime.getTime()) / HOUR_IN_MS);
                break;
            case MINUTE:
                count = (int) ((now.getTime() - startTime.getTime()) / MINUTE_IN_MS);
                break;
            case END_OF_MONTH:
            case END_OF_DAY:
            case NONE:
            default:
        }

        if (count > 2) {
            startCal.add(timeUnit.getCalendarUnit(), ((count - 2) / periodicity) * periodicity);
        }
        while (startCal.getTime().before(now)) {
            startCal.add(timeUnit.getCalendarUnit(), periodicity);
        }
        return startCal.getTime();
    }

    public static int getInstanceSequence(Date startTime, Frequency frequency, int periodicity, String timezone, Date instanceTime) {
        return getInstanceSequence(startTime, frequency.getTimeUnit(), periodicity, timezone, instanceTime);
    }

    public static int getInstanceSequence(Date startTime, TimeUnit timeUnit, int periodicity, String timezone, Date instanceTime) {

        if (startTime.after(instanceTime))
            return -1;

        Calendar startCal = Calendar.getInstance(EntityUtil.getTimeZone(timezone));
        startCal.setTime(startTime);

        int count = 0;
        switch (timeUnit) {
            case MONTH:
                count = (int) ((instanceTime.getTime() - startTime.getTime()) / MONTH_IN_MS);
                break;
            case DAY:
                count = (int) ((instanceTime.getTime() - startTime.getTime()) / DAY_IN_MS);
                break;
            case HOUR:
                count = (int) ((instanceTime.getTime() - startTime.getTime()) / HOUR_IN_MS);
                break;
            case MINUTE:
                count = (int) ((instanceTime.getTime() - startTime.getTime()) / MINUTE_IN_MS);
                break;
            case END_OF_MONTH:
            case END_OF_DAY:
            case NONE:
            default:
        }

        if (count > 2) {
            startCal.add(timeUnit.getCalendarUnit(), (count / periodicity) * periodicity);
            count = (count / periodicity);
        }
        while (startCal.getTime().before(instanceTime)) {
            startCal.add(timeUnit.getCalendarUnit(), periodicity);
            count++;
        }
        return count + 1;
    }

    public static String getStagingPath(Entity entity) throws IvoryException {
        try {
            byte[] digest = DigestUtils.md5(entity.toString());
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
}
