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

package org.apache.falcon.regression.core.util;

import org.apache.falcon.resource.LineageGraphResult;
import org.apache.log4j.Logger;
import org.testng.Assert;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 *Util function related to entity lineage.
 */
public final class EntityLineageUtil{

    private static final Logger LOGGER = Logger.getLogger(EntityLineageUtil.class);

    private EntityLineageUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    /**
     * Validates entity lineage results.
     * @param lineageGraphResult entity lineage result
     * @param expectedVertices array of expected vertices
     * @param expectedEdgeArray array of expected edges
     */
    public static void validateLineageGraphResult(LineageGraphResult lineageGraphResult, String[] expectedVertices,
                                                  LineageGraphResult.Edge[] expectedEdgeArray) {
        String[] actualVertices;
        LineageGraphResult.Edge[] actualEdgeArray;
        actualVertices = lineageGraphResult.getVertices();
        actualEdgeArray = lineageGraphResult.getEdges();

        Set<LineageGraphResult.Edge> expectedEdgeSet = new HashSet<>(Arrays.asList(expectedEdgeArray));
        Set<LineageGraphResult.Edge> actualEdgeSet = new HashSet<>(Arrays.asList(actualEdgeArray));

        Set<String> expectedVerticesSet = new HashSet<>(Arrays.asList(expectedVertices));
        Set<String> actualVerticesSet = new HashSet<>(Arrays.asList(actualVertices));

        Assert.assertEquals(expectedEdgeSet, actualEdgeSet, "Edges dont match");
        Assert.assertEquals(expectedVerticesSet, actualVerticesSet, "Vertices dont match");
    }

}

