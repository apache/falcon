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

/**
 * Moves Falcon logs.
 */
public class LogMoverService implements WorkflowExecutionListener{
    public static final String ENABLE_POSTPROCESSING = StartupProperties.get().
                getProperty("falcon.postprocessing.enable");

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException{
        if (!Boolean.parseBoolean(ENABLE_POSTPROCESSING)){
            new JobLogMover().moveLog(context);
        }
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException{
        onSuccess(context);
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException{
       //Do Nothin
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException{
        //DO Nothing
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException{
        //DO Nothing
    }
}
