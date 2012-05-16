package org.apache.ivory.util;

import org.apache.ivory.IvoryException;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class DeploymentProperties extends ApplicationProperties {
    private static final String PROPERTY_FILE = "deploy.properties";

    private static final AtomicReference<DeploymentProperties> instance =
        new AtomicReference<DeploymentProperties>();

    private DeploymentProperties() throws IvoryException {
      super();
    }

    @Override
    protected String getPropertyFile() {
      return PROPERTY_FILE;
    }

    public static Properties get() {
      try {
        if (instance.get() == null) {
          instance.compareAndSet(null, new DeploymentProperties());
        }
        return instance.get();
      } catch (IvoryException e) {
        throw new RuntimeException("Unable to read application " +
            "startup properties", e);
      }
    }
}
