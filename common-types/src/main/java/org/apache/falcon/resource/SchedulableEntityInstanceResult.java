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

package org.apache.falcon.resource;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Arrays;

/**
 * Instances list used for marshalling / unmarshalling with REST calls.
 */
@XmlRootElement(name = "instances")
@XmlAccessorType(XmlAccessType.FIELD)
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class SchedulableEntityInstanceResult extends APIResult {

    @XmlElement(name = "instances")
    private SchedulableEntityInstance[] instances;

    //For JAXB
    private SchedulableEntityInstanceResult() {
        super();
    }

    public SchedulableEntityInstanceResult(Status status, String message) {
        super(status, message);
    }

    public SchedulableEntityInstance[] getInstances() {
        return instances;
    }

    public void setInstances(SchedulableEntityInstance[] instances) {
        this.instances = instances;
    }


    @Override
    public Object[] getCollection() {
        return getInstances();
    }

    @Override
    public void setCollection(Object[] items) {
        if (items == null) {
            setInstances(new SchedulableEntityInstance[0]);
        } else {
            SchedulableEntityInstance[] newInstances = new SchedulableEntityInstance[items.length];
            for (int index = 0; index < items.length; index++) {
                newInstances[index] = (SchedulableEntityInstance)items[index];
            }
            setInstances(newInstances);
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        if (instances != null) {
            Arrays.sort(instances);
            for (SchedulableEntityInstance element : instances) {
                buffer.append(element.toString());
                buffer.append("\n");
            }
        }
        return buffer.toString();
    }
}
