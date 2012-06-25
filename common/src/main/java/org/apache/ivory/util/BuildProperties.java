package org.apache.ivory.util;

import org.apache.ivory.IvoryException;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class BuildProperties extends ApplicationProperties {
    private static final String PROPERTY_FILE = "ivory-buildinfo.properties";

    private static final AtomicReference<BuildProperties> instance =
        new AtomicReference<BuildProperties>();

    private BuildProperties() throws IvoryException {
      super();
    }

    @Override
    protected String getPropertyFile() {
      return PROPERTY_FILE;
    }

    public static Properties get() {
      try {
        if (instance.get() == null) {
          instance.compareAndSet(null, new BuildProperties());
        }
        return instance.get();
      } catch (IvoryException e) {
        throw new RuntimeException("Unable to read application " +
            "ivory build information properties", e);
      }
    }
}
