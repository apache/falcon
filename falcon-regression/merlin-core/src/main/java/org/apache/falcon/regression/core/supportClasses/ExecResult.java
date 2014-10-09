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

public final class ExecResult {

    private final int exitVal;
    private final String output;
    private final String error;

    public ExecResult(final int exitVal, final String output, final String error) {
        this.exitVal = exitVal;
        this.output = output;
        this.error = error;
    }

    public int getExitVal() {
        return exitVal;
    }

    public String getOutput() {
        return output;
    }

    public String getError() {
        return error;
    }
}
