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
package org.apache.falcon.rerun.queue;

import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.rerun.event.RerunEvent;
import org.apache.falcon.rerun.event.RerunEventFactory;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;

public class InMemoryQueue<T extends RerunEvent> extends DelayedQueue<T> {
    public static final Logger LOG = Logger.getLogger(DelayedQueue.class);
    protected DelayQueue<T> QUEUE = new DelayQueue<T>();
    private File serializeFilePath;

    @Override
    public boolean offer(T event) {
        boolean flag = QUEUE.offer(event);
        beforeRetry(event);
        LOG.debug("Enqueued Message:" + event.toString());
        return flag;
    }

    @Override
    public T take() throws FalconException {
        T event;
        try {
            event = QUEUE.take();
            LOG.debug("Dequeued Message:" + event.toString());
            afterRetry(event);
        } catch (InterruptedException e) {
            throw new FalconException(e);
        }
        return event;
    }

    public InMemoryQueue(File serializeFilePath) {
        this.serializeFilePath = serializeFilePath;
    }

    public void populateQueue(List<T> events) {
        for (T event : events) {
            QUEUE.offer(event);
        }

    }

    @Override
    public void init() {
        List<T> events = bootstrap();
        populateQueue(events);

    }

    @Override
    public void reconnect() throws FalconException {
        //Do Nothing
    }

    private void beforeRetry(T event) {
        File retryFile = getRetryFile(serializeFilePath, event);
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(retryFile,
                    true));
            out.write(event.toString());
            out.newLine();
            out.close();
        } catch (IOException e) {
            LOG.warn(
                    "Unable to write entry for process-instance: "
                            + event.getEntityName() + ":"
                            + event.getInstance(), e);
        }
    }

    private File getRetryFile(File basePath, T event) {
        return new File(basePath, (event.getType().name()) + "-"
                + event.getEntityName() + "-"
                + event.getInstance().replaceAll(":", "-"));
    }

    private void afterRetry(T event) {
        File retryFile = getRetryFile(serializeFilePath, event);
        if (!retryFile.exists()) {
            LOG.warn("Rerun file deleted or renamed for process-instance: "
                    + event.getEntityName() + ":" + event.getInstance());
            GenericAlert.alertRetryFailed(event.getEntityType(),
                    event.getEntityName(), event.getInstance(),
                    event.getWfId(), Integer.toString(event.getRunId()),
                    "Rerun file deleted or renamed for process-instance:");
        } else {
            if (!retryFile.delete()) {
                LOG.warn("Unable to remove rerun file " + event.getWfId());
                retryFile.deleteOnExit();
            }
        }
    }

    private List<T> bootstrap() {
        List<T> rerunEvents = new ArrayList<T>();
        for (File rerunFile : this.serializeFilePath.listFiles()) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(
                        rerunFile));
                String line;
                while ((line = reader.readLine()) != null) {
                    line.split("");
                    T event = new RerunEventFactory<T>().getRerunEvent(
                            rerunFile.getName(), line);
                    rerunEvents.add(event);
                }
            } catch (Exception e) {
                LOG.warn(
                        "Not able to read rerun entry "
                                + rerunFile.getAbsolutePath(), e);
            }
        }
        return rerunEvents;

    }
}
