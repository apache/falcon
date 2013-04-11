package org.apache.falcon;

import org.apache.falcon.util.EmbeddedServer;

public class Prism {

    public static void main(String[] args) throws Exception {
        EmbeddedServer server = new EmbeddedServer(16000,
                "prism/target/falcon-prism-0.2-SNAPSHOT");
        server.start();
    }
}
