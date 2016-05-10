package org.apache.falcon.plugin;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.apache.falcon.aspect.ResourceMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by praveen on 29/4/16.
 */
public class GraphitePlugin implements MonitoringPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(GraphitePlugin.class);

    @Override
    public void monitor(ResourceMessage message) {

    }


}
