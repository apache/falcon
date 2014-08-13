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

package org.apache.falcon.designer.primitive;

import org.apache.falcon.designer.configuration.FlowConfig;

/**
 * Concrete implementation for a Flow.
 */
public class Flow extends Primitive<Flow, FlowConfig> {

    private FlowConfig process;
    private String nameSpace;
    private String entity;

    public Flow(FlowConfig process, String nameSpace, String entity) {
        this.process = process;
        this.nameSpace = nameSpace;
        this.entity = entity;
    }

    @Override
    protected Flow copy() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterable<Message> validate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Code doCompile() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Flow doOptimize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getNamespace() {
        return nameSpace;
    }

    @Override
    public String getEntity() {
        return entity;
    }


    @Override
    public void setConfiguration(FlowConfig config) {
        this.process = config;
    }

    @Override
    public FlowConfig getConfiguration() {
        return process;
    }

}
