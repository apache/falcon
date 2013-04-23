/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon;

import org.apache.falcon.util.EmbeddedServer;

/**
 * Driver for running Prism as a standalone server.
 */
public final class Prism {

    /**
     * Default port number for embedded server.
     */
    private static final int PORT = 16000;

    /**
     * Prevent users from constructing this.
     */
    private Prism() {
    }

    /**
     * Main method.
     *
     * @param args argument array
     * @throws Exception exception
     */
    public static void main(final String[] args) throws Exception {
        EmbeddedServer server = new EmbeddedServer(PORT,
                "prism/target/falcon-prism-0.2-SNAPSHOT");
        server.start();
    }
}
