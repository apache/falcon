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

package org.apache.falcon.resource.channel;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.net.util.KeyManagerUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.log4j.Logger;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.security.SecureRandom;
import java.util.Properties;

/**
 * This supports prism server to falcon server over https.
 */
public class SecureHTTPChannel extends HTTPChannel {
    private static final Logger LOG = Logger.getLogger(SecureHTTPChannel.class);

    @Override
    protected Client getClient() throws Exception {
        Properties properties = StartupProperties.get();
        String keyStoreFile = properties.getProperty("keystore.file", "conf/prism.keystore");
        String password = properties.getProperty("keystore.password", "falcon-prism-passwd");
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(
                new KeyManager[]{KeyManagerUtils.createClientKeyManager(new File(keyStoreFile), password)},
                new TrustManager[] {TrustManagerUtils.getValidateServerCertificateTrustManager()},
                new SecureRandom());
        DefaultClientConfig config = new DefaultClientConfig();
        config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                new HTTPSProperties(new AllowAllHostnameVerifier(), sslContext));
        LOG.info("Configuring client with " + new File(keyStoreFile).getAbsolutePath());
        return Client.create(config);
    }
}
