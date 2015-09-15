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

package org.apache.falcon.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;

import java.io.IOException;
import java.util.List;

/**
 * The GroupsService class delegates to the Hadoop's <code>org.apache.hadoop.security.Groups</code>
 * to retrieve the groups a user belongs to.
 */
public class GroupsService implements FalconService {
    private org.apache.hadoop.security.Groups hGroups;

    public static final String SERVICE_NAME = GroupsService.class.getSimpleName();

    /**
     * Initializes the service.
     */
    @Override
    public void init() {
        hGroups = new Groups(new Configuration(true));
    }

    /**
     * Destroys the service.
     */
    @Override
    public void destroy() {
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    /**
     * Returns the list of groups a user belongs to.
     *
     * @param user user name.
     * @return the groups the given user belongs to.
     * @throws java.io.IOException thrown if there was an error retrieving the groups of the user.
     */
    public List<String> getGroups(String user) throws IOException {
        return hGroups.getGroups(user);
    }

}
