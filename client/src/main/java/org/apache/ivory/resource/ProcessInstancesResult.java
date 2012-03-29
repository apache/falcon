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
    
    @XmlRootElement (name = "pinstance")
    public static class ProcessInstance {
        @XmlElement
        public String instance;
        @XmlElement
        public WorkflowStatus status;
        
        public ProcessInstance() {}
        
        public ProcessInstance(String instance, WorkflowStatus status) {
            this.instance = instance;
            this.status = status;
        }
        
        public String getInstance() {
            return instance;
        }
        
        public WorkflowStatus getStatus() {
            return status;
        }
    }
    
    @XmlElement
    private ProcessInstance[] instances;

    private ProcessInstancesResult() { // for jaxb
        super();
    }

    public ProcessInstancesResult(String message, Map<String, String> instMap) {
        super(Status.SUCCEEDED, message);
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


	public ProcessInstance[] getInstances() {
        return instances;
    }

    public void setInstances(ProcessInstance[] instances) {
        this.instances = instances;
    }
}