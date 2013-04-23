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
package org.apache.falcon.rerun.handler;

import org.apache.falcon.rerun.event.LaterunEvent;
import org.apache.falcon.rerun.event.RerunEvent.RerunType;
import org.apache.falcon.rerun.event.RetryEvent;
import org.apache.falcon.rerun.queue.DelayedQueue;

/**
 * A factory implementation to dole out rerun handlers.
 */
public final class RerunHandlerFactory {

    private static final RetryHandler<DelayedQueue<RetryEvent>> RETRY_HANDLER
        = new RetryHandler<DelayedQueue<RetryEvent>>();
    private static final LateRerunHandler<DelayedQueue<LaterunEvent>> LATE_HANDLER
        = new LateRerunHandler<DelayedQueue<LaterunEvent>>();

    private RerunHandlerFactory() {
    }

    public static AbstractRerunHandler getRerunHandler(RerunType type) {
        switch (type) {
        case RETRY:
            return RETRY_HANDLER;

        case LATE:
            return LATE_HANDLER;

        default:
            throw new RuntimeException("Invalid handler:" + type);
        }
    }
}
