package org.apache.ivory.resource;

import java.util.Iterator;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.ivory.Pair;

@XmlRootElement
public class ProcessInstancesResult extends APIResult {
    public static enum WorkflowStatus {
        RUNNING, SUSPENDED, KILLED, FAILED;
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

    public ProcessInstancesResult(String message, Set<Pair<String, String>> insts) {
        super(Status.SUCCEEDED, message);
        if(insts != null) {
            instances = new ProcessInstance[insts.size()];
            Iterator<Pair<String, String>> itr = insts.iterator();
            int index = 0;
            while(itr.hasNext()) {
                Pair<String, String> pair = itr.next();
                instances[index++] = new ProcessInstance(pair.first, WorkflowStatus.valueOf(pair.second));
            }
        }
    }

    public ProcessInstancesResult(String message, Set<String> insts, WorkflowStatus status) {
        super(Status.SUCCEEDED, message);
        if(insts != null) {
            instances = new ProcessInstance[insts.size()];
            Iterator<String> itr = insts.iterator();
            int index = 0;
            while(itr.hasNext()) {
                String inst = itr.next();
                instances[index++] = new ProcessInstance(inst, status);
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