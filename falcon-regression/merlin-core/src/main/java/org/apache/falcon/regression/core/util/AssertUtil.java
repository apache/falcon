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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * Util methods for assert.
 */
public final class AssertUtil {

    private AssertUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    private static final Logger LOGGER = Logger.getLogger(AssertUtil.class);

    /**
     * Checks that any path in list doesn't contains a string.
     *
     * @param paths list of paths
     * @param shouldNotBePresent string that shouldn't be present
     */
    public static void failIfStringFoundInPath(
        List<Path> paths, String... shouldNotBePresent) {
        for (Path path : paths) {
            for (String aShouldNotBePresent : shouldNotBePresent) {
                if (path.toUri().toString().contains(aShouldNotBePresent)) {
                    Assert.fail("String " + aShouldNotBePresent + " was not expected in path "
                            +
                            path.toUri().toString());
                }
            }
        }
    }

    /**
     * Checks that two lists have same size.
     *
     * @param expected expected list
     * @param actual   actual list
     */
    public static void checkForListSizes(List<?> expected, List<?> actual) {
        if (expected.size() != actual.size()) {
            LOGGER.info("expected = " + expected);
        }
        checkForListSize(actual, expected.size());
    }

    /**
     * Checks that two lists have same size.
     *
     * @param elements list of elements
     * @param expectedSize expected size of the list
     */
    public static void checkForListSize(List<?> elements, int expectedSize) {
        if (elements.size() != expectedSize) {
            LOGGER.info("expectedSize = " + expectedSize);
            LOGGER.info("elements.size() = " + elements.size());
            LOGGER.info("elements = " + elements);
        }
        Assert.assertEquals(elements.size(), expectedSize,
            "Size of expected and actual list don't match.");
    }

    /**
     * Checks that two lists has expected diff element.
     *
     * @param initialState first list
     * @param finalState   second list
     * @param filename     expected diff element
     * @param expectedDiff diff count (positive for new elements)
     */
    public static void compareDataStoreStates(List<String> initialState,
                                              List<String> finalState, String filename,
                                              int expectedDiff) {

        if (expectedDiff > -1) {
            finalState.removeAll(initialState);
            Assert.assertEquals(finalState.size(), expectedDiff);
            if (expectedDiff != 0) {
                Assert.assertTrue(finalState.get(0).contains(filename));
            }
        } else {
            expectedDiff = expectedDiff * -1;
            initialState.removeAll(finalState);
            Assert.assertEquals(initialState.size(), expectedDiff);
            if (expectedDiff != 0) {
                Assert.assertTrue(initialState.get(0).contains(filename));
            }
        }


    }

    /**
     * Checks that two lists has expected diff element.
     *
     * @param initialState first list
     * @param finalState   second list
     * @param expectedDiff diff count (positive for new elements)
     */
    public static void compareDataStoreStates(List<String> initialState,
                                              List<String> finalState, int expectedDiff) {

        if (expectedDiff > -1) {
            finalState.removeAll(initialState);
            Assert.assertEquals(finalState.size(), expectedDiff);

        } else {
            expectedDiff = expectedDiff * -1;
            initialState.removeAll(finalState);
            Assert.assertEquals(initialState.size(), expectedDiff);

        }


    }

    /**
     * Checks that ServiceResponse status is SUCCEEDED.
     *
     * @param response ServiceResponse
     * @throws JAXBException
     */
    public static void assertSucceeded(ServiceResponse response) throws JAXBException {
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
            APIResult.Status.SUCCEEDED, "Status should be SUCCEEDED");
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200,
            "Status code should be 200");
        Assert.assertNotNull(Util.parseResponse(response).getMessage(), "Status message is null");
    }

    /**
     * Checks that ProcessInstancesResult status is SUCCEEDED.
     *
     * @param response ProcessInstancesResult
     */
    public static void assertSucceeded(InstancesResult response) {
        Assert.assertNotNull(response.getMessage());
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED,
            "Status should be SUCCEEDED");
    }

    /**
     * Checks that ServiceResponse status is status FAILED.
     *
     * @param response ServiceResponse
     * @param message  message for exception
     * @throws JAXBException
     */
    public static void assertFailed(final ServiceResponse response, final String message)
        throws JAXBException {
        assertFailedWithStatus(response, 400, message);
    }

    /**
     * Checks that ServiceResponse status is status FAILED with some status code.
     *
     * @param response   ServiceResponse
     * @param statusCode expected status code
     * @param message    message for exception
     * @throws JAXBException
     */
    public static void assertFailedWithStatus(final ServiceResponse response, final int statusCode,
                                              final String message) throws JAXBException {
        Assert.assertNotEquals(response.getMessage(), "null", "response message should not be null");
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
            APIResult.Status.FAILED, message);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), statusCode,
            message);
        Assert.assertNotNull(Util.parseResponse(response).getRequestId(), "RequestId is null");
    }

    /**
     * Checks that ServiceResponse status is status PARTIAL.
     *
     * @param response ServiceResponse
     * @throws JAXBException
     */
    public static void assertPartial(ServiceResponse response) throws JAXBException {
        Assert.assertEquals(Util.parseResponse(response).getStatus(), APIResult.Status.PARTIAL);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }

    /**
     * Checks that ServiceResponse status is status FAILED with status code 400.
     *
     * @param response ServiceResponse
     * @throws JAXBException
     */
    public static void assertFailed(ServiceResponse response) throws JAXBException {
        Assert.assertNotEquals(response.getMessage(), "null", "response message should not be null");

        Assert.assertEquals(Util.parseResponse(response).getStatus(), APIResult.Status.FAILED);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
    }

    /**
     * Checks that status of some entity job is equal to expected. Method can wait
     * 100 seconds for expected status.
     *
     * @param oozieClient    OozieClient
     * @param entityType     FEED or PROCESS
     * @param data           feed or proceess XML
     * @param expectedStatus expected Job.Status of entity
     * @throws OozieClientException
     */
    public static void checkStatus(OozieClient oozieClient, EntityType entityType, String data,
                                   Job.Status expectedStatus)
        throws OozieClientException {
        String name = null;
        if (entityType == EntityType.FEED) {
            name = Util.readEntityName(data);
        } else if (entityType == EntityType.PROCESS) {
            name = Util.readEntityName(data);
        }
        Assert.assertEquals(
            OozieUtil.verifyOozieJobStatus(oozieClient, name, entityType, expectedStatus), true,
            "Status should be " + expectedStatus);
    }

    /**
     * Checks that status of some entity job is equal to expected. Method can wait
     * 100 seconds for expected status.
     *
     * @param oozieClient    OozieClient
     * @param entityType     FEED or PROCESS
     * @param bundle         Bundle with feed or process data
     * @param expectedStatus expected Job.Status of entity
     * @throws OozieClientException
     */
    public static void checkStatus(OozieClient oozieClient, EntityType entityType, Bundle bundle,
                                   Job.Status expectedStatus)
        throws OozieClientException {
        String data = null;
        if (entityType == EntityType.FEED) {
            data = bundle.getDataSets().get(0);
        } else if (entityType == EntityType.PROCESS) {
            data = bundle.getProcessData();
        }
        checkStatus(oozieClient, entityType, data, expectedStatus);
    }

    /**
     * Checks that status of some entity job is NOT equal to expected.
     *
     * @param oozieClient    OozieClient
     * @param entityType     FEED or PROCESS
     * @param data           feed or proceess XML
     * @param expectedStatus expected Job.Status of entity
     * @throws OozieClientException
     */
    public static void checkNotStatus(OozieClient oozieClient, EntityType entityType, String data,
                                      Job.Status expectedStatus)
        throws OozieClientException {
        String processName = null;
        if (entityType == EntityType.FEED) {
            processName = Util.readEntityName(data);
        } else if (entityType == EntityType.PROCESS) {
            processName = Util.readEntityName(data);
        }
        Assert.assertNotEquals(OozieUtil.getOozieJobStatus(oozieClient, processName,
            entityType), expectedStatus, "Status should not be " + expectedStatus);
    }

    /**
     * Checks that status of some entity job is NOT equal to expected.
     *
     * @param oozieClient    OozieClient
     * @param entityType     FEED or PROCESS
     * @param bundle         Bundle with feed or process data
     * @param expectedStatus expected Job.Status of entity
     * @throws OozieClientException
     */
    public static void checkNotStatus(OozieClient oozieClient, EntityType entityType,
                                      Bundle bundle, Job.Status expectedStatus)
        throws OozieClientException {
        String data = null;
        if (entityType == EntityType.FEED) {
            data = bundle.getDataSets().get(0);
        } else if (entityType == EntityType.PROCESS) {
            data = bundle.getProcessData();
        }
        checkNotStatus(oozieClient, entityType, data, expectedStatus);
    }

    /**
     * Checks size of the content a two locations.
     *
     * @param firstPath  path to the first location
     * @param secondPath path to the second location
     * @param fs         hadoop file system for the locations
     * @throws IOException
     */
    public static void checkContentSize(String firstPath, String secondPath, FileSystem fs) throws
        IOException {
        final ContentSummary firstSummary = fs.getContentSummary(new Path(firstPath));
        final ContentSummary secondSummary = fs.getContentSummary(new Path(secondPath));
        LOGGER.info(firstPath + " : firstSummary = " + firstSummary.toString(false));
        LOGGER.info(secondPath + " : secondSummary = " + secondSummary.toString(false));
        Assert.assertEquals(firstSummary.getLength(), secondSummary.getLength(),
            "Contents at the two locations don't have same size.");
    }

    /**
     * Fail the test because of the supplied exception.
     * @param e exception
     */
    public static void fail(Exception e) {
        LOGGER.info("Got exception: " + ExceptionUtils.getStackTrace(e));
        Assert.fail("Failing because of exception.");
    }
}
