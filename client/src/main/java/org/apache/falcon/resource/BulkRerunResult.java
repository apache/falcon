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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * Pojo for JAXB marshalling / unmarshalling.
 */
//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class BulkRerunResult extends APIResult{

    /**
     * RestAPI supports filterBy these fields of instanceSummary.
     */
    public static enum BulkRerunFilterFields {
        STATUS, TAGS, PIPELINES, NAME
    }

    @XmlElement
    private List<InstancesResult> instancesResultList;

    public BulkRerunResult() {
        super();
    }

    public BulkRerunResult(Status status, String message) {
        super(status, message);
    }

    public List<InstancesResult> getInstancesResultList() {
        return instancesResultList;
    }

    public void setInstancesResultList(List<InstancesResult> instancesResultList) {
        this.instancesResultList = instancesResultList;
    }

    @Override
    public Object[] getCollection() {
        return getInstancesResultList().toArray(new InstancesResult[getInstancesResultList().size()]);
    }

    @Override
    public void setCollection(Object[] items) {
        if (items == null) {
            setInstancesResultList(new ArrayList<InstancesResult>());
        } else {
            List<InstancesResult> newInstances = new ArrayList<>();
            for (int index = 0; index < items.length; index++) {
                newInstances.add((InstancesResult)items[index]);
            }
            setInstancesResultList(newInstances);
        }
    }
}
