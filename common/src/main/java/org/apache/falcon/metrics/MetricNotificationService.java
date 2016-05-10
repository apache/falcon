package org.apache.falcon.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by praveen on 7/5/16.
 */
public class MetricNotificationService implements FalconService {
    private static final Logger LOG = LoggerFactory.getLogger(MetricNotificationService.class);

    public static final String SERVICE_NAME = MetricNotificationService.class.getSimpleName();
    private final GraphiteReporter graphiteReporter ;
    private final MetricRegistry metricRegistry;

    private static String PREFIX = "falcon";
    private Map<String,MyGauge> metricMap = new ConcurrentHashMap<>();

    public MetricNotificationService(){
        Graphite graphite = new Graphite(new InetSocketAddress("graphite.example.com", 2003));
        metricRegistry=new MetricRegistry();
        this.graphiteReporter = GraphiteReporter.forRegistry(metricRegistry)
                .prefixedWith(PREFIX)
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
        graphiteReporter.start(1, TimeUnit.SECONDS);
    }

    @Override
    public void destroy() throws FalconException {
        graphiteReporter.stop();
    }

    private MyGauge createMetric(String metricName){
        return metricMap.computeIfAbsent(metricName, new Function<String, MyGauge>() {
            @Override
            public MyGauge apply(String s) {
                return new MyGauge();
            }
        });
    }

    public void publish(String metricsName,Double value){
        createMetric(metricsName).setValue(value);
    }

    private static class MyGauge implements Gauge<Double> {

        private Double value=0d;
        public void setValue(Double value){
            this.value=value;
        }

        @Override
        public Double getValue() {
            return value;
        }
    }

}
