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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.feed.RetentionStage;
import org.apache.falcon.entity.v0.feed.Sla;
import org.apache.falcon.expression.ExpressionHelper;

import java.util.Date;

/**
 * Retention policy which deletes all instances of instance time older than a given time.
 * It will create the workflow and coordinators for this policy.
 */
public class AgeBasedDelete extends RetentionPolicy {

    public static final String LIMIT_PROPERTY_NAME = "retention.policy.agebaseddelete.limit";

    @Override
    public void validate(Feed feed, String clusterName) throws FalconException {
        // validate that it is a valid cluster
        Cluster cluster = FeedHelper.getCluster(feed, clusterName);
        Frequency retentionLimit = getRetentionLimit(feed, clusterName);
        if (cluster != null) {
            validateLimitWithSla(feed, cluster, retentionLimit.toString());
            validateLimitWithLateData(feed, cluster, retentionLimit.toString());
        }
    }

    private void validateLimitWithLateData(Feed feed, Cluster cluster, String retention) throws FalconException {
        ExpressionHelper evaluator = ExpressionHelper.get();
        long retentionPeriod = evaluator.evaluate(retention, Long.class);

        if (feed.getLateArrival() != null) {
            String feedCutoff = feed.getLateArrival().getCutOff().toString();
            long feedCutOffPeriod = evaluator.evaluate(feedCutoff, Long.class);
            if (retentionPeriod < feedCutOffPeriod) {
                throw new ValidationException(
                        "Feed's retention limit: " + retention + " of referenced cluster " + cluster.getName()
                                + " should be more than feed's late arrival cut-off period: " + feedCutoff
                                + " for feed: " + feed.getName());
            }
        }
    }

    private void validateLimitWithSla(Feed feed, Cluster cluster, String retentionExpression) throws FalconException {
        // test that slaHigh is less than retention
        Sla clusterSla = FeedHelper.getSLA(cluster, feed);
        if (clusterSla != null) {
            ExpressionHelper evaluator = ExpressionHelper.get();
            ExpressionHelper.setReferenceDate(new Date());

            Frequency slaHighExpression = clusterSla.getSlaHigh();
            Date slaHigh = new Date(evaluator.evaluate(slaHighExpression.toString(), Long.class));

            Date retention = new Date(evaluator.evaluate(retentionExpression, Long.class));
            if (slaHigh.after(retention)) {
                throw new ValidationException("slaHigh of Feed: " + slaHighExpression
                        + " is greater than retention of the feed: " + retentionExpression
                        + " for cluster: " + cluster.getName()
                );
            }
        }
    }

    public Frequency getRetentionLimit(Feed feed, String clusterName) throws FalconException {
        RetentionStage retention = FeedHelper.getRetentionStage(feed, clusterName);
        if (retention != null) {
            String limit = null;
            for (Property property : retention.getProperties().getProperties()) {
                if (StringUtils.equals(property.getName(), LIMIT_PROPERTY_NAME)) {
                    limit = property.getValue();
                }
            }
            if (limit == null) {
                throw new FalconException("Property: " + LIMIT_PROPERTY_NAME + " is required for "
                        + getName() + " policy.");
            }
            try {
                return new Frequency(limit);
            } catch (IllegalArgumentException e) {
                throw new FalconException("Invalid value for property: " + LIMIT_PROPERTY_NAME + ", should be a valid "
                        + "frequency e.g. hours(2)", e);
            }
        } else {
            throw new FalconException("Cluster " + clusterName + " doesn't contain retention stage");
        }
    }

}
