package org.apache.ivory;

import org.apache.ivory.util.EmbeddedServer;

public class Prism {

    public static void main(String[] args) throws Exception {
        EmbeddedServer server = new EmbeddedServer(16000,
                "prism/target/ivory-prism-0.2-SNAPSHOT");
        server.start();
    }
}
