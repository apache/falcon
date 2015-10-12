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
package org.apache.falcon.unit;

import org.apache.falcon.LifeCycle;
import org.apache.falcon.resource.AbstractInstanceManager;
import org.apache.falcon.resource.InstancesResult;

import java.util.List;

/**
 * A proxy implementation of the entity instance operations.
 */
public class LocalInstanceManager extends AbstractInstanceManager {

    public LocalInstanceManager() {}

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public InstancesResult getStatusOfInstances(String type, String entity, String start, String end,
                                                String colo, List<LifeCycle> lifeCycles, String filterBy,
                                                String orderBy, String sortOrder, Integer offset,
                                                Integer numResults) {
        return super.getStatus(type, entity, start, end, colo, lifeCycles, filterBy, orderBy, sortOrder,
                offset, numResults);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

}
