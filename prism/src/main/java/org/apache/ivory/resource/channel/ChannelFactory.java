package org.apache.ivory.resource.channel;

import java.util.HashMap;
import java.util.Map;

public class ChannelFactory {

    private static Map<String, Channel> channels = new HashMap<String, Channel>();

    public synchronized static Channel get(String serviceName) {
        if (true) {
            Channel channel;
            if ((channel = channels.get(serviceName)) == null) {
                channel = new IPCChannel(serviceName);
                channels.put(serviceName, channel);
            }
            return channel;
        } else {
            throw new UnsupportedOperationException(serviceName);
        }
    }
}
