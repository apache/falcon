package org.apache.ivory.resource.channel;

import org.apache.ivory.IvoryException;
import org.apache.ivory.resource.proxy.SchedulableEntityManagerProxy;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class HTTPChannel implements Channel {
    private static final Logger LOG = Logger.getLogger(HTTPChannel.class);

    @Override
    public <T> T invoke(String methodName, Object... args) throws IvoryException {

        for (Method method : SchedulableEntityManagerProxy.class.getDeclaredMethods()) {
            for (Annotation annotation : method.getDeclaredAnnotations()) {

            }
        }
        return null;
    }
}
