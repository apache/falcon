package org.apache.ivory.resource.channel;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.util.ApplicationProperties;

import java.util.HashMap;
import java.util.Map;

public class ChannelFactory {

    private static Map<String, Channel> channels = new HashMap<String, Channel>();

    private static final ApplicationProperties deploymentProperties;
    private static final String INTEGRATED = "integrated";
    private static final String MODE = "deploy.mode";

    static {
        try {
            deploymentProperties = new ApplicationProperties() {

                        @Override
                        protected String getPropertyFile() {
                            return "deploy.properties";
                        }
                    };
        } catch (IvoryException e) {
            throw new IvoryRuntimException(e);
        }

    }

    public synchronized static Channel get(String serviceName)
            throws IvoryException {

        Channel channel;
        if ((channel = channels.get(serviceName)) == null) {
            channel = getChannel(deploymentProperties.getProperty(MODE));
            channel.init(deploymentProperties, serviceName);
            channels.put(serviceName, channel);
        }
        return channel;
    }

    private static Channel getChannel(String mode) {
        Channel channel;
        if (mode.equals(INTEGRATED)) {
            channel = new IPCChannel();
        } else {
            channel = new HTTPChannel();
        }
        return channel;
    }
}
