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
package org.apache.falcon.util;

/**
 * Storage for Backlog Metrics.
 */
public class MetricInfo {

    private String nominalTime;
    private String cluster;

    public MetricInfo(String nominalTimeStr, String clusterName) {
        this.nominalTime = nominalTimeStr;
        this.cluster = clusterName;
    }

    public String getNominalTime() {
        return nominalTime;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !o.getClass().equals(this.getClass())) {
            return false;
        }

        MetricInfo other = (MetricInfo) o;

        boolean nominalTimeEqual = this.getNominalTime() != null
                ? this.getNominalTime().equals(other.getNominalTime()) : other.getNominalTime() == null;

        boolean clusterEqual = this.getCluster() != null
                ? this.getCluster().equals(other.getCluster()) : other.getCluster() == null;

        return this == other
                || (nominalTimeEqual && clusterEqual);
    }

    @Override
    public int hashCode() {
        int result = nominalTime != null ? nominalTime.hashCode() : 0;
        result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
        return result;
    }

    public String toString() {
        return "Nominaltime: " + this.getNominalTime() + " cluster: " + this.getCluster();
    }


}
