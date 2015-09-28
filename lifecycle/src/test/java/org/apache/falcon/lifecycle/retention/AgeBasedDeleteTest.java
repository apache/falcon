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

package org.apache.falcon.lifecycle.retention;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LateArrival;
import org.apache.falcon.entity.v0.feed.Lifecycle;
import org.apache.falcon.entity.v0.feed.Properties;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.feed.RetentionStage;
import org.apache.falcon.entity.v0.feed.Sla;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for AgeBasedDelete Policy validations.
 */
public class AgeBasedDeleteTest {
    private static Feed feed;
    private static String clusterName = "testCluster";

    @BeforeMethod
    private void init() {
        feed = new Feed();
        Cluster cluster = new Cluster();
        cluster.setName(clusterName);

        Property property = new Property();
        property.setName(AgeBasedDelete.LIMIT_PROPERTY_NAME);
        property.setValue("hours(3)");

        Properties properties = new Properties();
        properties.getProperties().add(property);

        RetentionStage retentionStage = new RetentionStage();
        retentionStage.setProperties(properties);

        Lifecycle lifecycle = new Lifecycle();
        lifecycle.setRetentionStage(retentionStage);

        cluster.setLifecycle(lifecycle);

        Clusters clusters = new Clusters();
        clusters.getClusters().add(cluster);
        feed.setClusters(clusters);

        //set sla
        Sla sla = new Sla();
        sla.setSlaLow(new Frequency("hours(3)"));
        sla.setSlaHigh(new Frequency("hours(3)"));
        feed.setSla(sla);

        // set late data arrival
        LateArrival lateArrival = new LateArrival();
        lateArrival.setCutOff(new Frequency("hours(3)"));
        feed.setLateArrival(lateArrival);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*slaHigh of Feed:.*")
    public void testSlaValidation() throws FalconException {
        feed.getSla().setSlaHigh(new Frequency("hours(4)"));
        new AgeBasedDelete().validate(feed, clusterName);
    }

    @Test(expectedExceptions = ValidationException.class,
    expectedExceptionsMessageRegExp = ".*Feed's retention limit:.*")
    public void testLateDataValidation() throws FalconException {
        feed.getLateArrival().setCutOff(new Frequency("hours(4)"));
        new AgeBasedDelete().validate(feed, clusterName);
    }

    @Test(expectedExceptions = FalconException.class,
        expectedExceptionsMessageRegExp = ".*Invalid value for property.*")
    public void testValidateLimit() throws FalconException {
        feed.getClusters().getClusters().get(0).getLifecycle().getRetentionStage().getProperties().getProperties()
                .get(0).setValue("invalid");
        new AgeBasedDelete().validate(feed, clusterName);
    }

    @Test(expectedExceptions = FalconException.class, expectedExceptionsMessageRegExp = ".*limit is required.*")
    public void testStageValidity() throws Exception {
        feed.getClusters().getClusters().get(0).getLifecycle().getRetentionStage().getProperties().getProperties()
                .clear();
        new AgeBasedDelete().validate(feed, clusterName);
    }
}
