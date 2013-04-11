package org.apache.falcon.resource.channel;

import org.apache.falcon.FalconException;

public interface Channel {

    void init(String colo, String serviceName) throws FalconException;

    <T> T invoke(String methodName, Object... args) throws FalconException;
}
