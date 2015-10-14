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

import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;

/**
 * A proxy implementation of the schedulable entity operations in local mode.
 */
public class LocalSchedulableEntityManager extends AbstractSchedulableEntityManager {
    // Created for future purposes to add all entity API's here for falcon unit.

    public LocalSchedulableEntityManager() {}

    public APIResult suspend(String type, String entity, String colo) {
        return super.suspend(null, type, entity, colo);
    }

    public APIResult resume(String type, String entity, String colo) {
        return super.resume(null, type, entity, colo);
    }

    public APIResult getStatus(String type, String entity, String colo) {
        return super.getStatus(type, entity, colo);
    }

}
