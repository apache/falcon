package org.apache.ivory.resource;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class InstancesResult extends APIResult {
	public static enum WorkflowStatus {
		WAITING, RUNNING, SUSPENDED, KILLED, FAILED, SUCCEEDED;
	}

	@XmlElement
    private Instance[] instances;

    private InstancesResult() { // for jaxb
        super();
    }
    
    public InstancesResult(String message, Instance[] instances) {
    	this(Status.SUCCEEDED, message, instances);
    }

    public InstancesResult(Status status, String message,
                           Instance[] instanceExes) {
    	super(status, message);
    	this.instances = instanceExes;
    }

	public InstancesResult(Status status, String message) {
	    super(status, message);
    }


    public Instance[] getInstances() {
        return instances;
    }

	public void setInstances(Instance[] instances) {
		this.instances = instances;
	}
	
	@XmlRootElement(name = "instance")
	public static class Instance {
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

		public Instance() {
		}

		public Instance(String cluster, String instance, WorkflowStatus status) {
			this.cluster = cluster;
			this.instance = instance;
			this.status = status;
		}

		public Instance(Instance instance, String logFile,
                        InstanceAction[] actions) {
			this.instance = instance.instance;
			this.status = instance.status;
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
			return status;
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
