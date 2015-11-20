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
package org.apache.falcon.notification.service.impl;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.exception.NotificationServiceException;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.execution.SchedulerUtil;
import org.apache.falcon.notification.service.FalconNotificationService;
import org.apache.falcon.notification.service.event.TimeElapsedEvent;
import org.apache.falcon.notification.service.request.NotificationRequest;
import org.apache.falcon.notification.service.request.AlarmRequest;
import org.apache.falcon.state.ID;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.CalendarIntervalTrigger;
import org.quartz.DateBuilder;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.quartz.CalendarIntervalScheduleBuilder.calendarIntervalSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * This notification service notifies {@link NotificationHandler} when requested time
 * event has occurred. The class users to subscribe to frequency based, cron based or some calendar based time events.
 */
public class AlarmService implements FalconNotificationService {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmService.class);

    private Map<ID, TriggerKey> notifications = new HashMap<ID, TriggerKey>();
    private static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
    private Scheduler scheduler;

    @Override
    public void init() throws FalconException {
        try {
            scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
        } catch (SchedulerException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public void register(NotificationRequest notificationRequest) throws NotificationServiceException {
        LOG.info("Registering alarm notification for " + notificationRequest.getCallbackId());
        AlarmRequest request = (AlarmRequest) notificationRequest;
        DateTime currentTime = DateTime.now();
        DateTime nextStartTime = request.getStartTime();
        DateTime endTime;
        if (request.getEndTime().isBefore(currentTime)) {
            endTime = request.getEndTime();
        } else {
            endTime = currentTime;
        }
        // Handle past events.
        // TODO : Quartz doesn't seem to support running jobs for past events.
        // TODO : Remove the handling of past events when that support is added.
        if (request.getStartTime().isBefore(currentTime)) {

            List<Date> instanceTimes = EntityUtil.getInstanceTimes(request.getStartTime().toDate(),
                    request.getFrequency(), request.getTimeZone(), request.getStartTime().toDate(),
                    endTime.toDate());
            if (instanceTimes != null && !instanceTimes.isEmpty()) {
                Date lastInstanceTime = instanceTimes.get(instanceTimes.size() - 1);
                nextStartTime = new DateTime(lastInstanceTime.getTime()
                        + SchedulerUtil.getFrequencyInMillis(new DateTime(lastInstanceTime), request.getFrequency()));
                // Introduce some delay to allow for rest of the registration to complete.
                LOG.debug("Triggering events for past from {} till {}", instanceTimes.get(0), lastInstanceTime);
                executor.schedule(new CatchupJob(request, instanceTimes), 1, TimeUnit.SECONDS);
            }
        }
        // All past events have been scheduled. Nothing to schedule in the future.
        if (request.getEndTime().isBefore(nextStartTime)) {
            return;
        }
        LOG.debug("Scheduling to trigger events from {} to {} with frequency {}", nextStartTime, request.getEndTime(),
                request.getFrequency());
        // Schedule future events using Quartz
        CalendarIntervalTrigger trigger = newTrigger()
                .withIdentity(notificationRequest.getCallbackId().toString(), "Falcon")
                .startAt(nextStartTime.toDate())
                .endAt(request.getEndTime().toDate())
                .withSchedule(
                        calendarIntervalSchedule()
                                .withInterval(request.getFrequency().getFrequencyAsInt(),
                                       getTimeUnit(request.getFrequency().getTimeUnit()))
                                .withMisfireHandlingInstructionFireAndProceed())
                .build();

        // define the job and tie it to our Job class
        JobDetail job = newJob(FalconProcessJob.class)
                .withIdentity(getJobKey(notificationRequest.getCallbackId().toString()))
                .setJobData(getJobDataMap((AlarmRequest) notificationRequest))
                .build();
        notifications.put(notificationRequest.getCallbackId(), trigger.getKey());
        // Tell quartz to run the job using our trigger
        try {
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            LOG.error("Error scheduling entity {}", trigger.getKey());
            throw new NotificationServiceException(e);
        }
    }

    // Maps the timeunit in entity specification to the one in Quartz DateBuilder
    private DateBuilder.IntervalUnit getTimeUnit(Frequency.TimeUnit timeUnit) {
        switch (timeUnit) {
        case minutes:
            return DateBuilder.IntervalUnit.MINUTE;
        case hours:
            return DateBuilder.IntervalUnit.HOUR;
        case days:
            return DateBuilder.IntervalUnit.DAY;
        case months:
            return DateBuilder.IntervalUnit.MONTH;
        default:
            throw new IllegalArgumentException("Invalid time unit " + timeUnit.name());
        }
    }

    private JobKey getJobKey(String key) {
        return new JobKey(key, "Falcon");
    }

    private JobDataMap getJobDataMap(AlarmRequest request) {
        JobDataMap jobProps = new JobDataMap();
        jobProps.put("request", request);

        return jobProps;
    }

    @Override
    public void unregister(NotificationHandler handler, ID listenerID) throws NotificationServiceException {
        try {
            LOG.info("Removing time notification for handler {} with callbackID {}", handler, listenerID);
            scheduler.unscheduleJob(notifications.get(listenerID));
            notifications.remove(listenerID);
        } catch (SchedulerException e) {
            throw new NotificationServiceException("Unable to deregister " + listenerID, e);
        }
    }

    @Override
    public RequestBuilder createRequestBuilder(NotificationHandler handler, ID callbackID) {
        return new AlarmRequestBuilder(handler, callbackID);
    }

    @Override
    public String getName() {
        return "AlarmService";
    }

    @Override
    public void destroy() throws FalconException {
        try {
            scheduler.shutdown();
        } catch (SchedulerException e) {
            LOG.warn("Quartz Scheduler shutdown failed.", e);
        }

    }

    // Generates a time elapsed event and invokes onEvent on the handler.
    private static void notifyHandler(AlarmRequest request, DateTime instanceTime) throws NotificationServiceException {
        TimeElapsedEvent event = new TimeElapsedEvent(request.getCallbackId(), request.getStartTime(),
                request.getEndTime(), instanceTime);
        try {
            LOG.info("Sending notification to {} with nominal time {} ", request.getCallbackId(),
                    event.getInstanceTime());
            request.getHandler().onEvent(event);
        } catch (FalconException e) {
            LOG.error("Unable to onEvent " + request.getCallbackId() + " for nominal time, " + instanceTime, e);
            throw new NotificationServiceException(e);
        }
    }

    /**
     * The Job that runs when a time trigger happens.
     */
    public static class FalconProcessJob implements Job {
        public FalconProcessJob() {
        }

        @Override
        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
            LOG.debug("Quartz job called at : {}, Next fire time: {}", jobExecutionContext.getFireTime(),
                    jobExecutionContext.getNextFireTime());

            AlarmRequest request = (AlarmRequest) jobExecutionContext.getJobDetail()
                    .getJobDataMap().get("request");
            DateTime instanceTime = new DateTime(jobExecutionContext.getScheduledFireTime(),
                    DateTimeZone.forTimeZone(request.getTimeZone()));

            try {
                notifyHandler(request, instanceTime);
            } catch (NotificationServiceException e) {
                throw new JobExecutionException(e);
            }
        }
    }

    // Quartz doesn't seem to be able to schedule past events. This job specifically handles that.
    private static class CatchupJob implements Runnable {

        private final AlarmRequest request;
        private final List<Date> instanceTimes;

        public CatchupJob(AlarmRequest request, List<Date> triggerTimes) {
            this.request = request;
            this.instanceTimes = triggerTimes;
        }

        @Override
        public void run() {
            if (instanceTimes == null) {
                return;
            }
            // Immediate notification for all past events.
            for(Date instanceTime : instanceTimes) {
                DateTime nominalDateTime = new DateTime(instanceTime, DateTimeZone.forTimeZone(request.getTimeZone()));
                try {
                    notifyHandler(request, nominalDateTime);
                } catch (NotificationServiceException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Builder that builds {@link AlarmRequest}.
     */
    public static class AlarmRequestBuilder extends RequestBuilder<AlarmRequest> {

        private DateTime startTime;
        private DateTime endTime;
        private Frequency frequency;
        private TimeZone timeZone;

        public AlarmRequestBuilder(NotificationHandler handler, ID callbackID) {
            super(handler, callbackID);
        }

        /**
         * @param start of the timer
         * @return This instance
         */
        public AlarmRequestBuilder setStartTime(DateTime start) {
            this.startTime = start;
            return this;
        }

        /**
         * @param end of the timer
         * @return This instance
         */
        public AlarmRequestBuilder setEndTime(DateTime end) {
            this.endTime = end;
            return this;
        }

        /**
         * @param freq of the timer
         * @return This instance
         */
        public AlarmRequestBuilder setFrequency(Frequency freq) {
            this.frequency = freq;
            return this;
        }

        /**
         * @param timeZone
         */
        public void setTimeZone(TimeZone timeZone) {
            this.timeZone = timeZone;
        }

        @Override
        public AlarmRequest build() {
            if (callbackId == null || startTime == null || endTime == null || frequency == null) {
                throw new IllegalArgumentException("Missing one or more of the mandatory arguments:"
                        + " callbackId, startTime, endTime, frequency");
            }
            return new AlarmRequest(handler, callbackId, startTime, endTime, frequency, timeZone);
        }
    }
}
