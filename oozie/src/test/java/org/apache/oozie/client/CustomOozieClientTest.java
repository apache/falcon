package org.apache.oozie.client;

import org.testng.annotations.Test;

import java.util.Properties;

public class CustomOozieClientTest {

    @Test
    public void testGetConfiguration() throws Exception {
        CustomOozieClient client = new CustomOozieClient("http://localhost:11000/oozie");
        Properties props = client.getConfiguration();
        System.out.println(props);
        props = client.getProperties();
        System.out.println(props);
    }
}
