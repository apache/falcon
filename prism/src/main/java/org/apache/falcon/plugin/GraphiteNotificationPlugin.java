package org.apache.falcon.plugin;

import org.apache.falcon.aspect.ResourceMessage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.metrics.MetricNotificationService;
import org.apache.falcon.service.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by praveen on 10/5/16.
 */
public class GraphiteNotificationPlugin implements MonitoringPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(GraphiteNotificationPlugin.class);
    @Override
    public void monitor(ResourceMessage message) {
        MetricNotificationService metricNotificationService =
                Services.get().getService(MetricNotificationService.SERVICE_NAME);
        try {
            String entityType = message.getDimensions().get("entity-type");
            String entityName = message.getDimensions().get("entity-name");
            Entity entity = null;
            if (entityType.equals("PROCESS")) {
                entity = ConfigurationStore.get().get(EntityType.PROCESS, entityName);
            }

            if ((message.getAction().equals("wf-instance-succeeded")
                    || message.getAction().equals("wf-instance-failed"))) {
               double timeTaken = (double) message.getExecutionTime() / 1000000000.0;
               String metricsName = "default.GENERATE" + entityName + ".processingTime";
               metricNotificationService.publish(metricsName,timeTaken);

               String startTime = message.getDimensions().get("start-time");
               String nominalTime = message.getDimensions().get("nominal-time");

            }
        } catch (Exception e) {
        LOG.error("Exception in sending Notification from EmailNotificationPlugin:" +e);
    }
    }
}
