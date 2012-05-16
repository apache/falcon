package org.apache.ivory.resource.channel;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.service.IvoryService;
import org.apache.ivory.service.Services;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.Properties;

public class IPCChannel extends AbstractChannel {
    private static final Logger LOG = Logger.getLogger(IPCChannel.class);

    private IvoryService service;

    public void init(Properties ignore, String serviceName)
            throws IvoryException{
        this.service = Services.get().init(serviceName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invoke(String methodName, Object... args)
            throws IvoryException {
        LOG.debug("Invoking method " + methodName + " on service " +
                service.getName());
        Method method = getMethod(service.getClass(), methodName, args);
        try {
            return (T) method.invoke(service, args);
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause != null)  {
                if (cause instanceof IvoryWebException) throw (IvoryWebException) cause;
                if (cause instanceof IvoryRuntimException) throw (IvoryRuntimException) cause;
                if (cause instanceof IvoryException) throw (IvoryException) cause;
            }
            throw new IvoryException("Unable to invoke on the channel " + methodName +
                    " on service : " + service.getName() + cause);
        }
    }
}
