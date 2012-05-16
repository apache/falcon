package org.apache.ivory.resource.channel;

import org.apache.ivory.IvoryException;
import org.apache.ivory.util.ApplicationProperties;

public interface Channel {

    void init(ApplicationProperties deploymentProperties,
              String serviceName) throws IvoryException;

    <T> T invoke(String methodName, Object... args) throws IvoryException;
}
