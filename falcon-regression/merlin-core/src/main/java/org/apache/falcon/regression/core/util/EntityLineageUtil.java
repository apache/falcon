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

import org.apache.falcon.regression.core.enumsAndConstants.ResponseErrors;
import org.apache.falcon.resource.LineageGraphResult;
import org.apache.falcon.resource.TriageResult;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 *Util function related to entity lineage.
 */
public final class EntityLineageUtil{

    private static final Logger LOGGER = Logger.getLogger(EntityLineageUtil.class);

    /**
     * Enum to represent entity role in pipeline.
     */
    public enum PipelineEntityType {
        PROCESS, INPUT_FEED, OUTPUT_FEED
    }

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
        Set<String> actualVerticesSet = new HashSet<>();
        Set<LineageGraphResult.Edge> actualEdgeSet = new HashSet<>();

        try {
            actualVertices = lineageGraphResult.getVertices();
            actualVerticesSet = new HashSet<>(Arrays.asList(actualVertices));
        } catch (NullPointerException e) {
            Assert.assertEquals(expectedVertices.length, 0);
        }
        try {
            actualEdgeArray = lineageGraphResult.getEdges();
            actualEdgeSet = new HashSet<>(Arrays.asList(actualEdgeArray));
        } catch (NullPointerException e) {
            Assert.assertEquals(expectedEdgeArray.length, 0);
        }

        Set<LineageGraphResult.Edge> expectedEdgeSet = new HashSet<>(Arrays.asList(expectedEdgeArray));
        Set<String> expectedVerticesSet = new HashSet<>(Arrays.asList(expectedVertices));

        Assert.assertEquals(actualEdgeSet, expectedEdgeSet, "Edges dont match");
        Assert.assertEquals(actualVerticesSet, expectedVerticesSet, "Vertices dont match");
    }

    /**
     * Validates that failed response contains specific error message.
     * @param triageResult response
     * @param error expected error
     */
    public static void validateError(TriageResult triageResult, ResponseErrors error) {
        AssertUtil.assertFailed(triageResult);
        Assert.assertTrue(triageResult.getMessage().contains(error.getError()),
                "Error should contain '" + error + "'");
    }

    /**
     * Produces list of expected vertices and edges in triage result.
     */
    public static LineageGraphResult getExpectedResult(int bundleIndx,
                                                       Map<PipelineEntityType, List<String>> entityNamesMap,
                                                       List<Integer> inputFeedFrequencies, String entityName,
                                                       String clusterName, String startTime) {
        List<String> processNames = entityNamesMap.get(PipelineEntityType.PROCESS);
        List<String> inputFeedNames = entityNamesMap.get(PipelineEntityType.INPUT_FEED);
        List<String> outputFeedNames = entityNamesMap.get(PipelineEntityType.OUTPUT_FEED);
        List<String> vertices = new ArrayList<>();
        List<LineageGraphResult.Edge> edges = new ArrayList<>();
        final String startTimeMinus20 = TimeUtil.addMinsToTime(startTime, -20);
        String vertexTemplate = "name: %s, type: %s, cluster: %s, instanceTime: %s, tags: %s";
        for (int i = 0; i <= bundleIndx; ++i) {
            //add vertex of i-th bundle process
            boolean isTerminalInstance = processNames.contains(entityName) && i == bundleIndx;
            String tag = isTerminalInstance ? "[WAITING]" : "Output[WAITING]";
            final String processVertex = String.format(vertexTemplate,
                processNames.get(i), "PROCESS", clusterName, startTime, tag);
            vertices.add(processVertex);

            //add all input feed vertices & edges for i-th bundle
            LineageGraphResult.Edge edge;
            String feedVertex;
            for (DateTime dt = new DateTime(startTime); !dt.isBefore(new DateTime(startTimeMinus20));
                 dt = dt.minusMinutes(inputFeedFrequencies.get(i))) {
                feedVertex = String.format(vertexTemplate, inputFeedNames.get(i), "FEED",
                    clusterName, TimeUtil.dateToOozieDate(dt.toDate()), "Input[MISSING]");
                edge = new LineageGraphResult.Edge(feedVertex, processVertex, "consumed by");
                vertices.add(feedVertex);
                edges.add(edge);
            }
            //add output feed edge for i-th bundle
            tag = (outputFeedNames.contains(entityName) && i == bundleIndx) ? "[MISSING]" : "Input[MISSING]";
            feedVertex = String.format(vertexTemplate, outputFeedNames.get(i), "FEED", clusterName, startTime, tag);
            isTerminalInstance = i == bundleIndx && outputFeedNames.contains(entityName);
            if (i < bundleIndx || isTerminalInstance) {
                edge = new LineageGraphResult.Edge(processVertex, feedVertex, "produces");
                edges.add(edge);
            }
            //add output feed vertex only if it is terminal; it will be added as the input for next bundle otherwise
            if (isTerminalInstance) {
                vertices.add(feedVertex);
            }
        }
        LineageGraphResult lineageGraphResult = new LineageGraphResult();
        lineageGraphResult.setVertices(vertices.toArray(new String[vertices.size()]));
        lineageGraphResult.setEdges(edges.toArray(new LineageGraphResult.Edge[edges.size()]));
        return lineageGraphResult;
    }

}

