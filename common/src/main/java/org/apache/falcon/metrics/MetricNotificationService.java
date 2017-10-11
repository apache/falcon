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

package org.apache.falcon.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.apache.falcon.FalconException;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.util.StartupProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Service for metrics notification.
 */
public class MetricNotificationService implements FalconService {
    private static final Logger LOG = LoggerFactory.getLogger(MetricNotificationService.class);

    public static final String SERVICE_NAME = MetricNotificationService.class.getSimpleName();
    private static final MetricNotificationService METRIC_NOTIFICATION_SERVICE = new MetricNotificationService();
    private final GraphiteReporter graphiteReporter;
    private final MetricRegistry metricRegistry;

    private Map<String, MetricGauge> metricMap = new ConcurrentHashMap<>();

    public static MetricNotificationService get(){
        return METRIC_NOTIFICATION_SERVICE;
    }

    public MetricNotificationService(){
        Graphite graphite = new Graphite(new InetSocketAddress(StartupProperties
                .get().getProperty("falcon.graphite.hostname"), Integer.parseInt(StartupProperties.get()
                    .getProperty("falcon.graphite.port"))));
        metricRegistry=new MetricRegistry();
        this.graphiteReporter = GraphiteReporter.forRegistry(metricRegistry)
                .convertDurationsTo(TimeUnit.SECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        LOG.info("Starting Graphite Service");
        graphiteReporter.start(Long.parseLong(StartupProperties.get().getProperty("falcon.graphite.frequency")),
                TimeUnit.SECONDS);
    }

    @Override
    public void destroy() throws FalconException {
        try {
            // reporting final metrics into graphite before stopping
            graphiteReporter.report();
        } finally {
            graphiteReporter.stop();
        }
    }

    private MetricGauge createMetric(final String metricName){
        if (!metricMap.containsKey(metricName)) {
            MetricGauge metricGauge = new MetricGauge();
            metricMap.put(metricName, metricGauge);
            metricRegistry.register(metricName, metricGauge);
        }
        return metricMap.get(metricName);
    }

    public void publish(String metricsName, Long value){
        synchronized(this){
            createMetric(metricsName).setValue(value);
        }
    }

    public void deleteMetric(String metricName){
        synchronized (this){
            SortedMap<String, Gauge> gaugeMap = metricRegistry.getGauges();
            if (gaugeMap.get(metricName) != null){
                metricRegistry.remove(metricName);
                metricMap.remove(metricName);
            }
        }
    }

    private static class MetricGauge implements Gauge<Long> {

        private Long value=0L;
        public void setValue(Long value){
            this.value=value;
        }

        @Override
        public Long getValue() {
            return value;
        }
    }
}
