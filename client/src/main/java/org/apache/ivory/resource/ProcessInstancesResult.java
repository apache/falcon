package org.apache.ivory.resource;

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
        public String cluster;

        @XmlElement
        public String startTime;

        @XmlElement
        public String endTime;

		@XmlElement
		public InstanceAction[] actions;

		public ProcessInstance() {
		}

		public ProcessInstance(String cluster, String instance, WorkflowStatus status) {
			this.cluster = cluster;
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

    
    public ProcessInstancesResult(String message, ProcessInstance[] processInstances) {
    	this(Status.SUCCEEDED, message, processInstances);
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