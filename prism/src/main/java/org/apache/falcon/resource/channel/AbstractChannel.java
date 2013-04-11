package org.apache.falcon.resource.channel;

import org.apache.falcon.FalconException;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractChannel implements Channel {

    private final ConcurrentHashMap<MethodKey, Method> methods
            = new ConcurrentHashMap<MethodKey, Method>();

    protected Method getMethod(Class service, String methodName, Object... args)
            throws FalconException {
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
            throw new FalconException("Lookup for " + methodKey +
                    " in service " + service.getName() + " found no match");
        }
        return method;
    }
}
