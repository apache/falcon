package org.apache.ivory.entity.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.ProcessType;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;

public class ConfigurationStoreTest {

  private static Logger LOG = Logger.getLogger(ConfigurationStoreTest.class);

  private ConfigurationStore store = ConfigurationStore.get();

  @Test
  public void testPublish() throws Exception {
    ProcessType process = new ProcessType();
    process.setName("hello");
    store.publish(EntityType.PROCESS, process);
    ProcessType p = store.get(EntityType.PROCESS, "hello");
    Assert.assertEquals(p, process);
  }

  @Test
  public void testGet() throws Exception {
    ProcessType p = store.get(EntityType.PROCESS, "notfound");
    Assert.assertNull(p);
  }

  @Test
  public void testRemove() throws Exception {
    ProcessType process = new ProcessType();
    process.setName("remove");
    store.publish(EntityType.PROCESS, process);
    ProcessType p = store.get(EntityType.PROCESS, "remove");
    Assert.assertEquals(p, process);
    store.remove(EntityType.PROCESS, "remove");
    p = store.get(EntityType.PROCESS, "remove");
    Assert.assertNull(p);
  }

  @Test
  public void testSearch() throws Exception {
    //TODO
  }

  @BeforeSuite
  @AfterSuite
  public void cleanup() throws IOException {
    Path path = new Path(StartupProperties.get().
        getProperty("config.store.uri"));
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    fs.delete(path, true);
    LOG.info("Cleaned up " + path);
  }
}
