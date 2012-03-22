package org.apache.ivory.service;

import org.apache.ivory.IvoryException;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * User: sriksun
 * Date: 20/03/12
 */
public final class Services implements Iterable<IvoryService> {
    private static final Logger LOG = Logger.getLogger(Services.class);

    private static Services instance = new Services();

    private Services() { }

    public static Services get() {
        return instance;
    }

    private final Map<String, IvoryService> services =
            new LinkedHashMap<String, IvoryService>();

    public synchronized void register(IvoryService service)
            throws IvoryException {
        if (services.containsKey(service.getName())) {
            throw new IvoryException("Service " + service.getName() +
                    " already registered");
        } else {
            services.put(service.getName(), service);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends IvoryService> T getService(String serviceName) {
        if (services.containsKey(serviceName)) {
            return (T) services.get(serviceName);
        } else {
            throw new NoSuchElementException("Service " + serviceName +
                    " not registered with registry");
        }
    }

    public boolean isRegistered(String serviceName) {
        return services.containsKey(serviceName);
    }

    @Override
    public Iterator<IvoryService> iterator() {
        return services.values().iterator();
    }
}
