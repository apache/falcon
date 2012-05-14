package org.apache.ivory.resource.channel;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.service.IvoryService;
import org.apache.ivory.service.Services;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class IPCChannel implements Channel {
    private static final Logger LOG = Logger.getLogger(IPCChannel.class);

    private final ConcurrentHashMap<String, Method> methods
            = new ConcurrentHashMap<String, Method>();
    private final IvoryService service;

    public IPCChannel(String serviceName) {
        try {
            this.service = Services.get().init(serviceName);
        } catch (IvoryException e) {
            LOG.error(e);
            throw new IvoryRuntimException("Unable to initialize channel", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invoke(String methodName, Object... args)
            throws IvoryException {
        LOG.debug("Invoking method " + methodName + " on service " +
                service.getName());
        Method method = getMethod(service, methodName, args);
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

    private Method getMethod(IvoryService service, String methodName, Object... args)
            throws IvoryException {
        Method method = methods.get(methodName);
        if (method == null) {
            Class[] argsClasses = new Class[args.length];
            for (int index = 0; index < args.length; index++) {
                if (args[index] == null) {
                    argsClasses[index] = null;
                } else {
                    argsClasses[index] = args[index].getClass();
                }
            }
            for (Method item : service.getClass().getDeclaredMethods()) {
                if (item.getName().endsWith(methodName) &&
                        item.getParameterTypes().length == argsClasses.length) {
                    boolean matching = true;
                    Class[] paramTypes = item.getParameterTypes();
                    for (int index = 0; index < argsClasses.length; index++) {
                        if (argsClasses[index] != null &&
                                !paramTypes[index].isAssignableFrom(argsClasses[index])) {
                             matching = false;
                        }
                    }
                    if (matching) {
                        methods.putIfAbsent(methodName, item);
                        return item;
                    }
                }
            }
            throw new IvoryException("Method: " + methodName + " is not found on " +
                    service.getName() + " with args type: " + Arrays.toString(argsClasses));
        }
        return method;
    }
}
