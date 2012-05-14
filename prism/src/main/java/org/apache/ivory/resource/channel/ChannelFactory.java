package org.apache.ivory.resource.channel;

import java.util.HashMap;
import java.util.Map;

public class ChannelFactory {

    private static Map<String, Channel> channels = new HashMap<String, Channel>();

    public synchronized static Channel get(String serviceName) {
        if (true) { //inproc channel
            Channel channel;
            if ((channel = channels.get(serviceName)) == null) {
                channel = new IPCChannel(serviceName);
                channels.put(serviceName, channel);
            }
            return channel;
        } else { //Remote channel
            throw new UnsupportedOperationException(serviceName);
        }
    }
}
