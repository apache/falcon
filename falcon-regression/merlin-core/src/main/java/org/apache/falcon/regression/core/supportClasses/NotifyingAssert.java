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

package org.apache.falcon.regression.core.supportClasses;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.testng.asserts.IAssert;
import org.testng.asserts.SoftAssert;
import org.testng.collections.Maps;

import java.util.Map;

/**
 * NotifyingAssert: This is same as SoftAssert provided by TestNg. Additionally, it adds an option
 * of printing stacktrace whenever test execution fails.
 */
public class NotifyingAssert extends SoftAssert {
    private final boolean printFailures;
    // LinkedHashMap to preserve the order
    private Map<AssertionError, IAssert> mErrors = Maps.newLinkedHashMap();
    private static final Logger LOGGER = Logger.getLogger(NotifyingAssert.class);

    /**
     * Same of SoftAssert - just adds an option for logging assertion failure stacktraces.
     * @param logFailures - switches on printing of stacktrace in logs on failures.
     */
    public NotifyingAssert(boolean logFailures) {
        this.printFailures = logFailures;
    }

    @Override
    public void executeAssert(IAssert a) {
        try {
            a.doAssert();
        } catch(AssertionError ex) {
            onAssertFailure(a, ex);
            mErrors.put(ex, a);
            if (printFailures) {
                LOGGER.info("Assertion failed - exception : " + ex + "\n"
                    + ExceptionUtils.getStackTrace(ex));
            }
        }
    }

    public void assertAll() {
        if (!mErrors.isEmpty()) {
            StringBuilder sb = new StringBuilder("The following asserts failed:\n");
            boolean first = true;
            for (Map.Entry<AssertionError, IAssert> ae : mErrors.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append(ae.getValue().getMessage());
            }
            throw new AssertionError(sb.toString());
        }
    }
}
