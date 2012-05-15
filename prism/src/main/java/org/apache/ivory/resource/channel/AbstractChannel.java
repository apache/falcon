package org.apache.ivory.resource.channel;

import org.apache.ivory.IvoryException;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractChannel implements Channel {

    private final ConcurrentHashMap<MethodKey, Method> methods
            = new ConcurrentHashMap<MethodKey, Method>();

    protected Method getMethod(Class service, String methodName, Object... args)
            throws IvoryException {
        MethodKey methodKey = new MethodKey(methodName,  args);
        Method method = methods.get(methodKey);
        if (method == null) {
            for (Method item : service.getDeclaredMethods()) {
                MethodKey itemKey = new MethodKey(item.getName(),
                        item.getParameterTypes());
                if (methodKey.equals(itemKey)) {
                    methods.putIfAbsent(methodKey, item);
                    return item;
                }
            }
            throw new IvoryException("Lookup for " + methodKey +
                    " in service " + service.getName() + " found no match");
        }
        return method;
    }
}
