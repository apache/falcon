/*
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

package org.apache.ivory.workflow.engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.log4j.Logger;

import java.io.IOException;

public class OozieHouseKeepingService implements WorkflowEngineActionListener {

    private static Logger LOG = Logger.getLogger(OozieHouseKeepingService.class);

    @Override
    public void beforeSchedule(Cluster cluster, Entity entity) throws IvoryException {
    }

    @Override
    public void afterSchedule(Cluster cluster, Entity entity) throws IvoryException {
    }

    @Override
    public void beforeDelete(Cluster cluster, Entity entity) throws IvoryException {
    }

    @Override
    public void afterDelete(Cluster cluster, Entity entity) throws IvoryException {
        Path workflowFolder = new Path(ClusterHelper.getCompleteLocation(cluster, "staging"), entity.getStagingPath()).getParent();
        try {
            FileSystem fs = workflowFolder.getFileSystem(new Configuration());
            LOG.info("Deleting workflow " + workflowFolder);
            if (fs.exists(workflowFolder) && !fs.delete(workflowFolder, true)) {
                throw new IvoryException("Unable to cleanup workflow xml; " + "delete failed " + workflowFolder);
            }
        } catch (IOException e) {
            throw new IvoryException("Unable to cleanup workflow xml", e);
        }
    }

    @Override
    public void beforeSuspend(Cluster cluster, Entity entity) throws IvoryException {
    }

    @Override
    public void afterSuspend(Cluster cluster, Entity entity) throws IvoryException {
    }

    @Override
    public void beforeResume(Cluster cluster, Entity entity) throws IvoryException {
    }

    @Override
    public void afterResume(Cluster cluster, Entity entity) throws IvoryException {
    }
}
