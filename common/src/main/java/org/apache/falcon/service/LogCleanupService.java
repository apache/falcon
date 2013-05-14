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

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.cleanup.AbstractCleanupHandler;
import org.apache.falcon.cleanup.FeedCleanupHandler;
import org.apache.falcon.cleanup.ProcessCleanupHandler;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.util.StartupProperties;
import org.apache.log4j.Logger;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Log cleanup service.
 */
public class LogCleanupService implements FalconService {

    private static final Logger LOG = Logger.getLogger(LogCleanupService.class);
    private final ExpressionEvaluator evaluator = new ExpressionEvaluatorImpl();
    private final ExpressionHelper resolver = ExpressionHelper.get();

    @Override
    public String getName() {
        return "Falcon Log cleanup service";
    }

    @Override
    public void init() throws FalconException {
        Timer timer = new Timer();
        timer.schedule(new CleanupThread(), 0, getDelay());
        LOG.info("Falcon log cleanup service initialized");

    }

    private static class CleanupThread extends TimerTask {

        private AbstractCleanupHandler processCleanupHandler = new ProcessCleanupHandler();
        private AbstractCleanupHandler feedCleanupHandler = new FeedCleanupHandler();

        @Override
        public void run() {
            try {
                LOG.info("Cleaning up logs at: " + new Date());
                processCleanupHandler.cleanup();
                feedCleanupHandler.cleanup();
            } catch (Throwable t) {
                LOG.error("Error in cleanup task: ", t);
                GenericAlert.alertLogCleanupServiceFailed(
                        "Exception in log cleanup service", t);
            }
        }
    }

    @Override
    public void destroy() throws FalconException {
        LOG.info("Falcon log cleanup service destroyed");
    }

    private long getDelay() throws FalconException {
        String delay = StartupProperties.get().getProperty(
                "falcon.cleanup.service.frequency", "days(1)");
        try {
            return (Long) evaluator.evaluate("${" + delay + "}", Long.class,
                    resolver, resolver);
        } catch (ELException e) {
            throw new FalconException("Exception in EL evaluation", e);
        }
    }

}
