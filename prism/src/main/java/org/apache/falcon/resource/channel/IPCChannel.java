package org.apache.falcon.resource.channel;

import java.lang.reflect.Method;

import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.resource.AbstractEntityManager;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.log4j.Logger;

public class IPCChannel extends AbstractChannel {
    private static final Logger LOG = Logger.getLogger(IPCChannel.class);
    private AbstractEntityManager service;

    public void init(String ignoreColo, String serviceName) throws FalconException {
        service = ReflectionUtils.getInstance(serviceName + ".impl");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invoke(String methodName, Object... args)
            throws FalconException {
        LOG.debug("Invoking method " + methodName + " on service " +
                service.getClass().getName());
        Method method = getMethod(service.getClass(), methodName, args);
        try {
            return (T) method.invoke(service, args);
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause != null)  {
                if (cause instanceof FalconWebException) throw (FalconWebException) cause;
                if (cause instanceof FalconRuntimException) throw (FalconRuntimException) cause;
                if (cause instanceof FalconException) throw (FalconException) cause;
            }
            throw new FalconException("Unable to invoke on the channel " + methodName +
                    " on service : " + service.getClass().getName() + cause);
        }
    }
}
