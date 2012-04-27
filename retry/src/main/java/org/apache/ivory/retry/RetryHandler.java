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
package org.apache.ivory.retry;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.DelayQueue;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.util.GenericAlert;
import org.apache.ivory.util.StartupProperties;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

public final class RetryHandler {

	private static final Logger LOG = Logger.getLogger(RetryHandler.class);
	private static final long MINUTES = 60 * 1000L;
	private static final long HOURS = 60 * MINUTES;
	private static final long DAYS = 24 * HOURS;
	private static final long MONTHS = 31 * DAYS;

	private static enum DELAYS {
		minutes, hours, days, months
	};

	private static final DelayQueue<RetryEvent> QUEUE = new DelayQueue<RetryEvent>();

	private static File basePath;

	public void retry(String processName, String nominalTime, String runId,
			TextMessage textMessage, String wfId,
			WorkflowEngine workflowEngine, long msgReceivedTime)
			throws IvoryException {

		try {
			Process processObj = getProcess(processName);
			if (!validate(processName, processObj)) {
				return;
			}

			int attempts = processObj.getRetry().getAttempts();
			int delay = processObj.getRetry().getDelay();
			String delayUnit = processObj.getRetry().getDelayUnit();
			String policy = processObj.getRetry().getPolicy();
			int intRunId = Integer.parseInt(runId);
			String ivoryDate = getIvoryDate(nominalTime);

			if (attempts > intRunId) {
				if (policy.equals("backoff")) {
					retryBackoff(delayUnit, delay, workflowEngine, processObj
							.getCluster().getName(), wfId, processName,
							ivoryDate, textMessage, intRunId, attempts,
							msgReceivedTime);
				} else if (policy.equals("exp-backoff")) {
					retryExpBackoff(delayUnit, delay, workflowEngine,
							processObj.getCluster().getName(), wfId,
							processName, ivoryDate, textMessage, intRunId,
							attempts, msgReceivedTime);
				}
			} else {
				LOG.warn("All retry attempt failed out of configured: "
						+ attempts + " attempt for process instance::"
						+ processName + ":" + nominalTime + " And WorkflowId: "
						+ wfId);

				GenericAlert.alertWFfailed(processName, nominalTime);
			}
		} catch (Exception e) {
			LOG.error(e);
			GenericAlert.alertRetryFailed(processName, nominalTime,
					Integer.parseInt(runId), e.getMessage());
			throw new IvoryException(e);
		}
	}

	private Process getProcess(String processName) throws IvoryException {
		return ConfigurationStore.get().get(EntityType.PROCESS, processName);
	}

	private static boolean validate(String processName, Process processObj) {
		if (processObj == null) {
			LOG.warn("Ignoring retry, as the process:" + processName
					+ " does not exists in config store");
			return false;
		}
		if (processObj.getRetry() == null) {
			LOG.warn("Retry not configured for the process: " + processName);
			return false;
		}
		return true;
	}

	private void retryBackoff(String delayUnit, int delay,
			WorkflowEngine workflowEngine, String clusterName, String wfId,
			String processName, String ivoryDate, TextMessage textMessage,
			int runId, int attempts, long msgReceivedTime)
			throws IvoryException, JMSException {
		long endOfDelay = getEndOfDealy(delayUnit, delay);
		RetryEvent event = new RetryEvent(workflowEngine, clusterName, wfId,
				msgReceivedTime, endOfDelay, processName, ivoryDate, runId,
				attempts, 0);
		offerToQueue(event);

	}

	private void retryExpBackoff(String delayUnit, int delay,
			WorkflowEngine workflowEngine, String clusterName, String wfId,
			String processName, String ivoryDate, TextMessage textMessage,
			int runId, int attempts, long msgReceivedTime)
			throws IvoryException, JMSException {

		long endOfDelay = (long) (getEndOfDealy(delayUnit, delay) * Math.pow(2,
				runId));
		RetryEvent event = new RetryEvent(workflowEngine, clusterName, wfId,
				msgReceivedTime, endOfDelay, processName, ivoryDate, runId,
				attempts, 0);
		offerToQueue(event);
	}

	private long getEndOfDealy(String delayUnit, int delay)
			throws IvoryException {

		if (delayUnit.equals(DELAYS.minutes.name())) {
			return MINUTES * delay;
		} else if (delayUnit.equals(DELAYS.hours.name())) {
			return HOURS * delay;
		} else if (delayUnit.equals(DELAYS.days.name())) {
			return DAYS * delay;
		} else if (delayUnit.equals(DELAYS.months.name())) {
			return MONTHS * delay;
		} else {
			throw new IvoryException("Unknown delayUnit:" + delayUnit);
		}
	}

	public String getIvoryDate(String nominalTime) throws ParseException {
		DateFormat nominalFormat = new SimpleDateFormat(
				"yyyy'-'MM'-'dd'-'HH'-'mm");
		Date nominalDate = nominalFormat.parse(nominalTime);
		DateFormat ivoryFormat = new SimpleDateFormat(
				"yyyy'-'MM'-'dd'T'HH':'mm'Z'");
		return ivoryFormat.format(nominalDate);

	}

	public static class Consumer extends Thread {
		@Override
		public void run() {
			while (true) {
				RetryEvent message = null;
				try {
					message = takeFromQueue();
				} catch (InterruptedException e) {
					LOG.error("RetryHandlerConsumer interrupted");
					return;
				}
				try {
					Process processObj = ConfigurationStore.get().get(
							EntityType.PROCESS, message.getProcessName());
					if (!validate(message.getProcessName(), processObj)) {
						return;
					}
					String jobStatus = message.getWfEngine().instanceStatus(
							message.getClusterName(), message.getWfId());
					if (!jobStatus.equals("KILLED")) {
						LOG.debug("Re-enqueing message in RetryHandler for workflow:"
								+ message.getWfId());
						offerToQueue(message);
						continue;
					}
					LOG.info("Retrying attempt:" + (message.getRunId() + 1)
							+ " out of configured: " + message.getAttempts()
							+ " attempt for process instance::"
							+ message.getProcessName() + ":"
							+ message.getProcessInstance()
							+ " And WorkflowId: " + message.getWfId()
							+ " At time: "
							+ getTZdate(new Date(System.currentTimeMillis())));
					message.getWfEngine().reRun(message.getClusterName(),
							message.getWfId(), null);
				} catch (IvoryException e) {
					int maxFailRetryCount = Integer.parseInt(StartupProperties
							.get().getProperty("max.retry.failure.count", "1"));
					if (message.getFailRetryCount() < maxFailRetryCount) {
						LOG.warn(
								"Retrying again for process instance "
										+ message.getProcessName()
										+ ":"
										+ message.getProcessInstance()
										+ " after "
										+ message.getEndOfDelay()
										+ " seconds as Retry failed with message:",
								e);
						message.setFailRetryCount(message.getFailRetryCount() + 1);
						offerToQueue(message);
					} else {
						GenericAlert.alertRetryFailed(message.getProcessName(),
								message.getProcessInstance(),
								message.getRunId(), e.getMessage());
					}

				}
			}
		}

		private String getTZdate(Date date) {
			DateFormat ivoryFormat = new SimpleDateFormat(
					"yyyy'-'MM'-'dd'T'HH':'mm'Z'");
			return ivoryFormat.format(date);
		}
	}

	public static void setBasePath() {
		basePath = new File(StartupProperties.get().getProperty(
				"retry.recorder.path", "/tmp/ivory/retry"));
		if ((!basePath.exists() && !basePath.mkdirs())
				|| (basePath.exists() && !basePath.canWrite())) {
			throw new RuntimeException("Unable to initialize retry recorder @"
					+ basePath);
		}
	}

	public static File getBasePath() {
		return basePath;
	}

	public static void afterRetry(RetryEvent event) {
		File retryFile = getRetryFile(basePath, event.getProcessName(),
				event.getProcessInstance());
		if (!retryFile.exists()) {
			LOG.warn("Retry file delete or renamed for process-instance: " + event.getProcessName()
					+ ":" + event.getProcessInstance());
			GenericAlert.alertRetryFailed(event.getProcessName(),  event.getProcessInstance(), event.getRunId(), "Retry file delete or renamed for process-instance");
		} else {
			if (!retryFile.delete()) {
				LOG.warn("Unable to remove retry file " + event.getWfId());
				retryFile.deleteOnExit();
			}
		}
	}

	public static void beforeRetry(RetryEvent event) {
		File retryFile = getRetryFile(basePath, event.getProcessName(),
				event.getProcessInstance());
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(retryFile,
					true));
			out.write(event.toString());
			out.newLine();
			out.close();
		} catch (IOException e) {
			LOG.warn(
					"Unable to write entry for process-instance: "
							+ event.getProcessName() + ":"
							+ event.getProcessInstance(), e);
		}
	}

	private static void offerToQueue(RetryEvent event) {
		QUEUE.offer(event);
		beforeRetry(event);
	}

	private static RetryEvent takeFromQueue() throws InterruptedException {
		RetryEvent event = QUEUE.take();
		afterRetry(event);
		return event;
	}

	public static void enqueue(RetryEvent event) {
		QUEUE.offer(event);
	}

	private static File getRetryFile(File basePath, String processName,
			String processInstance) {
		return new File(basePath, processName + "-" + processInstance.replaceAll(":", "-"));
	}

}
