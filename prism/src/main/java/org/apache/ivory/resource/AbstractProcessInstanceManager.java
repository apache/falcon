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

package org.apache.ivory.resource;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.parser.ValidationException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.logging.LogProvider;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.resource.ProcessInstancesResult.WorkflowStatus;
import org.apache.ivory.retry.RetryHandler;
import org.apache.ivory.transaction.TransactionManager;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.log4j.Logger;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public abstract class AbstractProcessInstanceManager extends AbstractEntityManager {
    private static final Logger LOG = Logger.getLogger(AbstractProcessInstanceManager.class);

    protected Process getProcess(String processName) throws IvoryException {
        Entity entity = getEntityObject(processName, EntityType.PROCESS.name());
        return (Process) entity;
    }
    
    public ProcessInstancesResult getRunningInstances(String processName, String colo) {
        checkColo(colo);
        try {
            validateNotEmpty("process", processName);
            WorkflowEngine wfEngine = getWorkflowEngine();
            Process process = getProcess(processName);
            Map<String, Set<String>> runInstances = wfEngine.getRunningInstances(process);
            return new ProcessInstancesResult("getRunningInstances is successful",
                    runInstances.values().iterator().next(), WorkflowStatus.RUNNING);
        } catch (Throwable e) {
            LOG.error("Failed to get running instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }


	public ProcessInstancesResult getStatus(String processName, String startStr, String endStr,
                                            String type, String runId, String colo) {
        checkColo(colo);
        try {
			validateParams(processName, startStr, endStr, type, runId);

			Date start = EntityUtil.parseDateUTC(startStr);
			Date end = getEndDate(start, endStr);
			Process process = getProcess(processName);

			WorkflowEngine wfEngine = getWorkflowEngine();
			Map<String, Map<String, String>> instances = wfEngine.getStatus(
					process, start, end);
			ProcessInstancesResult result = new ProcessInstancesResult(
					"getStatus is successful", instances.values().iterator()
							.next());
			return getProcessInstanceWithLog(process,
					Tag.valueOf(type == null ? Tag.DEFAULT.name() : type),
					runId == null ? "0" : runId, result);
		} catch (Throwable e) {
			LOG.error("Failed to get instances status", e);
			throw IvoryWebException
					.newException(e, Response.Status.BAD_REQUEST);
		}
	}

	private ProcessInstancesResult getProcessInstanceWithLog(Process process,
			Tag type, String runId, ProcessInstancesResult result)
			throws IvoryException {
		ProcessInstancesResult.ProcessInstance[] processInstances = new ProcessInstancesResult.ProcessInstance[result
				.getInstances().length];
		for (int i = 0; i < result.getInstances().length; i++) {
			ProcessInstancesResult.ProcessInstance pInstance = LogProvider
					.getLogUrl(process, result.getInstances()[i], type, runId);
			processInstances[i] = pInstance;
		}

		return new ProcessInstancesResult(result.getMessage(), processInstances);
	}

    public ProcessInstancesResult killProcessInstance(HttpServletRequest request,
            String processName, String startStr, String endStr, String colo) {

        checkColo(colo);
        try {
            TransactionManager.startTransaction();
            audit(request, processName, EntityType.PROCESS.name(), "INSTANCE_KILL");
            validateParams(processName, startStr, endStr);
            
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);            
            Process process = getProcess(processName);
            
            WorkflowEngine wfEngine = getWorkflowEngine();
            Map<String, Map<String, String>> killedInstances = wfEngine.killInstances(process, start, end);
            ProcessInstancesResult result = new ProcessInstancesResult("killProcessInstance is successful",
                    killedInstances.values().iterator().next());
            TransactionManager.commit();
            return result;
        } catch (Throwable e) {
            TransactionManager.rollback();
            LOG.error("Failed to kill instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    public ProcessInstancesResult suspendProcessInstance(HttpServletRequest request,
            String processName, String startStr, String endStr, String colo) {

        checkColo(colo);
        try {
            TransactionManager.startTransaction();
            audit(request, processName, EntityType.PROCESS.name(), "INSTANCE_SUSPEND");
            validateParams(processName, startStr, endStr);
            
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);            
            Process process = getProcess(processName);
            
            WorkflowEngine wfEngine = getWorkflowEngine();
            Map<String, Map<String, String>> suspendedInstances = wfEngine.suspendInstances(process, start, end);
            ProcessInstancesResult result = new ProcessInstancesResult("suspendProcessInstance is successful",
                    suspendedInstances.values().iterator().next());
            TransactionManager.commit();
            return result;
        } catch (Throwable e) {
            TransactionManager.rollback();
            LOG.error("Failed to suspend instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    public ProcessInstancesResult resumeProcessInstance(HttpServletRequest request,
            String processName, String startStr, String endStr, String colo) {

        checkColo(colo);
        try {
            TransactionManager.startTransaction();
            audit(request, processName, EntityType.PROCESS.name(), "INSTANCE_RESUME");
            validateParams(processName, startStr, endStr);
            
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);            
            Process process = getProcess(processName);
            
            WorkflowEngine wfEngine = getWorkflowEngine();
            Map<String, Map<String, String>> resumedInstances = wfEngine.resumeInstances(process, start, end);
            ProcessInstancesResult result = new ProcessInstancesResult("resumeProcessInstance is successful", resumedInstances.values().iterator().next());
            TransactionManager.commit();
            return result;
        } catch (Throwable e) {
            TransactionManager.rollback();
            LOG.error("Failed to resume instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    public ProcessInstancesResult reRunInstance(String processName, String startStr, String endStr,
                                                HttpServletRequest request, String colo) {

        checkColo(colo);
        try {
            TransactionManager.startTransaction();
            audit(request, processName, EntityType.PROCESS.name(), "INSTANCE_RERUN");
            validateParams(processName, startStr, endStr);
            
            Date start = EntityUtil.parseDateUTC(startStr);
            Date end = getEndDate(start, endStr);            
            Process process = getProcess(processName);
            
            Properties props = new Properties();
            ServletInputStream xmlStream = request==null?null:request.getInputStream();
            if (xmlStream != null) {
                if (xmlStream.markSupported()) {
                    xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
                }
                props.load(xmlStream);
            }

            WorkflowEngine wfEngine = getWorkflowEngine();
            Map<String, Map<String, String>> runInstances = wfEngine.reRunInstances(process, start, end, props);
            ProcessInstancesResult result = new ProcessInstancesResult("reRunProcessInstance is successful",
                    runInstances.values().iterator().next());
            TransactionManager.commit();
            return result;
        } catch (Exception e) {
            TransactionManager.rollback();
            LOG.error("Failed to rerun instances", e);
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }
    
    private Date getEndDate(Date start, String endStr) throws IvoryException {
        Date end;
        if (StringUtils.isEmpty(endStr)) {
            end = new Date(start.getTime() + 1000); // next sec
        } else
            end = EntityUtil.parseDateUTC(endStr);
        return end;
    }
    
    private void validateParams(String processName, String startStr, String endStr) throws IvoryException {
        validateNotEmpty("process", processName);
        validateNotEmpty("start", startStr);
        
        Process process = getProcess(processName);
        validateDateRange(process, startStr, endStr);
    }

    private void validateDateRange(Process process, String start, String end) throws IvoryException {
        Date procStart = EntityUtil.parseDateUTC(process.getValidity().getStart());
        Date procEnd = EntityUtil.parseDateUTC(process.getValidity().getEnd());
        
        Date instStart = EntityUtil.parseDateUTC(start);
        if(instStart.before(procStart))
            throw new ValidationException("Start date " + start + " is before process start " + process.getValidity().getStart());
        
        if(StringUtils.isNotEmpty(end)) {
            Date instEnd = EntityUtil.parseDateUTC(end);
            if(instStart.after(instEnd))
                throw new ValidationException("Start date " + start + " is after end date " + end);
            
            if(instEnd.after(procEnd))
                throw new ValidationException("End date " + end + " is after process end " + process.getValidity().getEnd());
        } else if(instStart.after(procEnd))
            throw new ValidationException("Start date " + start + " is after process end " + process.getValidity().getEnd());
            
    }

    private void validateNotEmpty(String field, String param) throws ValidationException {
        if (StringUtils.isEmpty(param))
            throw new ValidationException("Parameter " + field + " is empty");
    }
    
	private void validateParams(String processName, String startStr,
			String endStr, String type, String runId) throws IvoryException {
		validateParams(processName, startStr, endStr);
		if (type != null && !type.equalsIgnoreCase("DEFAULT")
				&& !type.equalsIgnoreCase("LATE1")) {
			throw new ValidationException("Invalid process type: " + type);
		}
		if (runId != null) {
			try {
				Integer.parseInt(runId);
			} catch (NumberFormatException e) {
				throw new ValidationException("Invalid runId:", e);
			}
		}
	}
    
	/*
	 * Below method is a mock and gets automatically invoked by Aspect
	 */
	// TODO capture execution time
	@Monitored(event = "process-instance")
	public String instrumentWithAspect(
			@Dimension(value = "process") String process,
			@Dimension(value = "feed") String feedName,
			@Dimension(value = "feedPath") String feedpath,
			@Dimension(value = "nominalTime") String nominalTime,
			@Dimension(value = "timeStamp") String timeStamp,
			@Dimension(value = "status") String status,
			@Dimension(value = "workflowId") String workflowId,
			@Dimension(value = "runId") String runId, long msgReceivedTime)
			throws Exception {
		if (status.equalsIgnoreCase("FAILED")) {
			new RetryHandler().retry(process, nominalTime, runId, workflowId,
					getWorkflowEngine(), msgReceivedTime);
			throw new Exception(process + ":" + nominalTime + " Failed");
		}
		return "DONE";

	}

}
