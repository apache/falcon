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

package org.apache.ivory.converter;

import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.DATAIN;
import org.apache.ivory.oozie.coordinator.DATAOUT;
import org.dozer.DozerConverter;

public class ProcessConverter extends DozerConverter<Process, COORDINATORAPP> {

    private static final String EL_PREFIX = "ivory:";

    public ProcessConverter() {
        super(Process.class, COORDINATORAPP.class);
    }

    @Override
    public COORDINATORAPP convertTo(Process Process, COORDINATORAPP coordinatorapp) {
        coordinatorapp.setFrequency("${coord:" + Process.getFrequency() + "(" + Process.getPeriodicity() + ")}");

        //Format into oozie EL
        if((coordinatorapp.getInputEvents() != null) && (coordinatorapp.getInputEvents().getDataIn() != null)) {
            for(DATAIN input:coordinatorapp.getInputEvents().getDataIn()) {
                input.setStartInstance(getELExpression(input.getStartInstance()));
                input.setEndInstance(getELExpression(input.getEndInstance()));
            }
        }
        if((coordinatorapp.getOutputEvents() != null) && (coordinatorapp.getOutputEvents().getDataOut() != null)) {
            for(DATAOUT output:coordinatorapp.getOutputEvents().getDataOut()) {
                output.setInstance(getELExpression(output.getInstance()));
            }
        }

        return coordinatorapp;
    }

    private String getELExpression(String expr) {
        if(expr != null) {
            expr = "${" + EL_PREFIX + expr + "}";
        }
        return expr;
    }

    @Override
    public Process convertFrom(COORDINATORAPP arg0, Process arg1) {
        // TODO Auto-generated method stub
        return null;
    }
}