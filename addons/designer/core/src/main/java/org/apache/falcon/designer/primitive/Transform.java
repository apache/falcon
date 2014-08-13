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

import org.apache.falcon.designer.configuration.TransformConfiguration;
import org.apache.falcon.designer.schema.RelationalData;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.List;

/**
 * Transform is a foundational primitive in Falcon designer. All well
 * understood data transformations are to be implemented as a Transform.
 *
 * A transform would typically consume one or more data inputs conforming
 * to a schema and would produce one or more output typically with an uniform schema.
 *
 */
public abstract class Transform extends Primitive<Transform , TransformConfiguration> {

    protected List<RelationalData> inputData;
    protected List<RelationalData> outputData;

    /** Empty constructor to be used only for deserialization
     * and cloning. Not otherwise.
     */
    protected Transform() {
    }

    /**
     * Setter typically used for deserialization & cloning.
     *
     * @param inputData - List of input data
     */
    protected void setInputData(@Nonnull List<RelationalData> inputData) {
        this.inputData = inputData;
    }

    /**
     * Setter typically used for deserialization & cloning.
     *
     * @param outputData - List of output data
     */
    protected void setOutputData(@Nonnull List<RelationalData> outputData) {
        this.outputData = outputData;
    }

    /**
     * Each Transform by default requires one or more input data sets
     * and produces a single output data set.
     *
     * @param inData - List of input data sets for this transform
     * @param outData - List of Output data produced by this transform
     */
    protected Transform(@Nonnull List<RelationalData> inData, @Nonnull List<RelationalData> outData) {
        inputData = Collections.unmodifiableList(inData);
        outputData = Collections.unmodifiableList(outData);
    }

    @Nonnull
    public List<RelationalData> getInputData() {
        return inputData;
    }

    @Nonnull
    public List<RelationalData> getOutputData() {
        return outputData;
    }

}
