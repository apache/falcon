package org.apache.ivory.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ProcessInstancesResult extends APIResult {
	public static enum WorkflowStatus {
		WAITING, RUNNING, SUSPENDED, KILLED, FAILED, SUCCEEDED;
	}

	@XmlRootElement(name = "pinstance")
	public static class ProcessInstance {
		@XmlElement
		public String instance;
		@XmlElement
		public WorkflowStatus status;
		@XmlElement
		public String logFile;

		@XmlElement
		public InstanceAction[] actions;

		public ProcessInstance() {
		}

		public ProcessInstance(String instance, WorkflowStatus status) {
			this.instance = instance;
			this.status = status;
		}

		public ProcessInstance(ProcessInstance processInstance, String logFile,
				InstanceAction[] actions) {
			this.instance = processInstance.instance;
			this.status = processInstance.status;
			this.logFile = logFile;
			this.actions = actions;
		}
        
        public String getInstance() {
            return instance;
        }
        
        public WorkflowStatus getStatus() {
            return status;
        }
        
		@Override
		public String toString() {
			return "{instance:" + this.instance + ", status:" + this.status
					+ (this.logFile == null ? "" : ", log:" + this.logFile)
					+ "}";
		}
    }
    
	@XmlElement
    private ProcessInstance[] instances;

    private ProcessInstancesResult() { // for jaxb
        super();
    }

    
    public ProcessInstancesResult(String message, Map<String, String> instMap) {
        this(message, instMap, null);
    }
    
    public ProcessInstancesResult(String message, Map<String, String> instMap, String requestId) {
        super(Status.SUCCEEDED, message, requestId);
        if(instMap != null) {
            instances = new ProcessInstance[instMap.size()];
            List<String> sortedInstances = new ArrayList<String>(instMap.keySet());
            Collections.sort(sortedInstances);
            int index = 0;
            for(String instance:sortedInstances) {
                instances[index++] = new ProcessInstance(instance, WorkflowStatus.valueOf(instMap.get(instance)));
            }
        }
    }

    public ProcessInstancesResult(String message, Set<String> insts, WorkflowStatus status) {
        super(Status.SUCCEEDED, message);
        if(insts != null) {
            instances = new ProcessInstance[insts.size()];
            List<String> sortedInstances = new ArrayList<String>(insts);
            Collections.sort(sortedInstances);
            int index = 0;
            for(String instance:sortedInstances) {
                instances[index++] = new ProcessInstance(instance, status);
            }
        }
    }

    public ProcessInstancesResult(String message,
    		ProcessInstance[] processInstanceExs) {
    	this(Status.SUCCEEDED, message, processInstanceExs);
    }

    public ProcessInstancesResult(Status status, String message,
    		ProcessInstance[] processInstanceExs) {
    	super(status, message);
    	this.instances = processInstanceExs;
    }

	public ProcessInstance[] getInstances() {
        return instances;
    }

	public void setInstances(ProcessInstance[] instances) {
		this.instances = instances;
	}

	@XmlRootElement(name = "actions")
	public static class InstanceAction {
		@XmlElement
		public String action;
		@XmlElement
		public String status;
		@XmlElement
		public String logFile;

		public InstanceAction() {
		}

		public InstanceAction(String action, String status, String logFile) {
			this.action = action;
			this.status = status;
			this.logFile = logFile;
		}

		public String getAction() {
			return action;
		}

		public String getStatus() {
			return action;
		}

		public String getLogFile() {
			return logFile;
		}

		@Override
		public String toString() {
			return "{action:" + this.action + ", status:" + this.status
					+ (this.logFile == null ? "" : ", log:" + this.logFile)
					+ "}";
		}
	}
}