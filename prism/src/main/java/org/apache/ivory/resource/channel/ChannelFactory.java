package org.apache.ivory.resource.channel;

import org.apache.ivory.IvoryException;
import org.apache.ivory.util.DeploymentProperties;

import java.util.HashMap;
import java.util.Map;

public class ChannelFactory {

    private static Map<String, Channel> channels = new HashMap<String, Channel>();

    private static final String EMBEDDED = "embedded";
    private static final String MODE = "deploy.mode";

    public synchronized static Channel get(String serviceName, String colo)
            throws IvoryException {     
        Channel channel;
        if ((channel = channels.get(colo + "/" + serviceName)) == null) {
            channel = getChannel(DeploymentProperties.get().getProperty(MODE));
            channel.init(colo, serviceName);
            channels.put(colo + "/" + serviceName, channel);
        }
        return channel;
    }

    private static Channel getChannel(String mode) {
        Channel channel;
        if (mode.equals(EMBEDDED)) {
            channel = new IPCChannel();
        } else {
            channel = new HTTPChannel();
        }
        return channel;
    }
}
