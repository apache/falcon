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

import org.apache.falcon.FalconException;
import org.apache.falcon.logging.JobLogMover;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Moves Falcon logs.
 */
public class LogMoverService implements WorkflowExecutionListener  {

    private static final Logger LOG = LoggerFactory.getLogger(LogMoverService.class);

    public static final String ENABLE_POSTPROCESSING = StartupProperties.get().
            getProperty("falcon.postprocessing.enable");

    private BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(Integer.parseInt(
            StartupProperties.get().getProperty("falcon.logMoveService.blockingQueue.length", "50")));
    private ExecutorService executorService = new ThreadPoolExecutor(getCorePoolSize(), getThreadCount(), 120,
            TimeUnit.SECONDS, blockingQueue);

    public int getCorePoolSize(){
        try{
            return Integer.parseInt(StartupProperties.get().getProperty("falcon.logMoveService.min.threadCount", "20"));
        } catch (NumberFormatException  e){
            LOG.error("Exception in LogMoverService", e);
            return 20;
        }
    }
    public int getThreadCount() {
        try{
            return Integer.parseInt(StartupProperties.get()
                    .getProperty("falcon.logMoveService.max.threadCount", "200"));
        } catch (NumberFormatException  e){
            LOG.error("Exception in LogMoverService", e);
            return 200;
        }
    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException{
        onEnd(context);
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException{
        onEnd(context);
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException{
       //Do Nothing
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException{
        //DO Nothing
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException{
        //DO Nothing
    }

    private void onEnd(WorkflowExecutionContext context){
        if (Boolean.parseBoolean(ENABLE_POSTPROCESSING)) {
            return;
        }
        while(blockingQueue.remainingCapacity()<=0){
            try {
                LOG.trace("Sleeping, no capacity in threadpool....");
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                LOG.error("Exception in LogMoverService", e);
            }
        }
        executorService.execute(new LogMover(context));
    }


    private static class LogMover implements Runnable {
        private WorkflowExecutionContext context;
        public LogMover(@Nonnull WorkflowExecutionContext context){
            this.context = context;
        }
        @Override
        public void run(){
            new JobLogMover().moveLog(context);
        }
    }

}
