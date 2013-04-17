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
package org.apache.falcon.cleanup;

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;
import java.io.IOException;

/**
 * Falcon cleanup handler for cleaning up work, temp and log files
 * left behind by falcon.
 */
public abstract class AbstractCleanupHandler {

    protected static final Logger LOG = Logger
            .getLogger(AbstractCleanupHandler.class);
    protected static final ConfigurationStore STORE = ConfigurationStore.get();
    public static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    public static final ExpressionHelper RESOLVER = ExpressionHelper.get();

    protected long getRetention(Entity entity, TimeUnit timeUnit)
        throws FalconException {

        String retention = getRetentionValue(timeUnit);
        try {
            return (Long) EVALUATOR.evaluate("${" + retention + "}",
                    Long.class, RESOLVER, RESOLVER);
        } catch (ELException e) {
            throw new FalconException("Unable to evalue retention limit: "
                    + retention + " for entity: " + entity.getName());
        }
    }

    private String getRetentionValue(Frequency.TimeUnit timeunit) {
        return RuntimeProperties.get().getProperty(
                "log.cleanup.frequency." + timeunit + ".retention", "days(1)");

    }

    protected FileStatus[] getAllLogs(org.apache.falcon.entity.v0.cluster.Cluster cluster, Entity entity)
        throws FalconException {

        String stagingPath = ClusterHelper.getLocation(cluster, "staging");
        Path logPath = getLogPath(entity, stagingPath);
        FileSystem fs = getFileSystem(cluster);
        FileStatus[] paths;
        try {
            paths = fs.globStatus(logPath);
        } catch (IOException e) {
            throw new FalconException(e);
        }
        return paths;
    }

    private FileSystem getFileSystem(org.apache.falcon.entity.v0.cluster.Cluster cluster)
        throws FalconException {

        FileSystem fs;
        try {
            fs = new Path(ClusterHelper.getStorageUrl(cluster))
                    .getFileSystem(new Configuration());
        } catch (IOException e) {
            throw new FalconException(e);
        }
        return fs;
    }

    protected void delete(Cluster cluster, Entity entity, long retention)
        throws FalconException {

        FileStatus[] logs = getAllLogs(cluster, entity);
        long now = System.currentTimeMillis();

        for (FileStatus log : logs) {
            if (now - log.getModificationTime() > retention) {
                try {
                    boolean isDeleted = getFileSystem(cluster).delete(
                            log.getPath(), true);
                    if (!isDeleted) {
                        LOG.error("Unable to delete path: " + log.getPath());
                    } else {
                        LOG.info("Deleted path: " + log.getPath());
                    }
                    deleteParentIfEmpty(getFileSystem(cluster), log.getPath().getParent());
                } catch (IOException e) {
                    throw new FalconException(" Unable to delete log file : "
                            + log.getPath() + " for entity " + entity.getName()
                            + " for cluster: " + cluster.getName(), e);
                }
            } else {
                LOG.info("Retention limit: " + retention
                        + " is less than modification"
                        + (now - log.getModificationTime()) + " for path: "
                        + log.getPath());
            }
        }

    }

    private void deleteParentIfEmpty(FileSystem fs, Path parent) throws IOException {
        FileStatus[] files = fs.listStatus(parent);
        if (files != null && files.length == 0) {
            LOG.info("Parent path: " + parent + " is empty, deleting path");
            fs.delete(parent, true);
            deleteParentIfEmpty(fs, parent.getParent());
        }

    }

    public abstract void cleanup() throws FalconException;

    protected abstract Path getLogPath(Entity entity, String stagingPath);

    protected String getCurrentColo() {
        return StartupProperties.get().getProperty("current.colo", "default");
    }
}
