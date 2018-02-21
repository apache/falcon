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
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.proxy.BufferedRequest;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.DeploymentProperties;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Properties;

/**
 * A Channel implementation for HTTP.
 */
public class HTTPChannel extends AbstractChannel {
    private static final Logger LOG = LoggerFactory.getLogger(HTTPChannel.class);

    private static final HttpServletRequest DEFAULT_NULL_REQUEST = new NullServletRequest();

    private static final Properties DEPLOYMENT_PROPERTIES = DeploymentProperties.get();

    private static final String DO_AS_PARAM = "doAs";

    private String colo;
    private String serviceName;
    private Class service;

    /* Name of the HTTP cookie used for the authentication token between Prism and Falcon server.
     */
    private static final String AUTH_COOKIE = "hadoop.auth";
    private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";

    protected static final KerberosAuthenticator AUTHENTICATOR = new KerberosAuthenticator();
    protected static final HostnameVerifier ALL_TRUSTING_HOSTNAME_VERIFIER = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession sslSession) {
            return true;
        }
    };

    public void init(String inColo, String inServiceName) throws FalconException {
        this.colo = inColo;
        this.serviceName = inServiceName;
        try {
            String proxyClassName = DEPLOYMENT_PROPERTIES.getProperty(serviceName + ".proxy");
            service = Class.forName(proxyClassName);
            LOG.info("Service: {}", serviceName);
        } catch (Exception e) {
            throw new FalconException("Unable to initialize channel for " + serviceName, e);
        }
    }

    private String getFalconEndPoint() {
        String prefixPath = DEPLOYMENT_PROPERTIES.getProperty(serviceName + ".path");
        String falconEndPoint = RuntimeProperties.get().getProperty("falcon." + colo + ".endpoint");
        return falconEndPoint + "/" + prefixPath;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invoke(String methodName, Object... args) throws FalconException {
        HttpServletRequest incomingRequest = null;
        try {
            Method method = getMethod(service, methodName, args);
            String urlPrefix = getFalconEndPoint();
            final String url = urlPrefix + "/" + pathValue(method, args);
            LOG.debug("Executing {}", url);

            incomingRequest = getIncomingRequest(args);
            incomingRequest.getInputStream().reset();
            String httpMethod = getHttpMethod(method);
            String mimeType = getConsumes(method);
            String accept = MediaType.WILDCARD;
            final String user = CurrentUser.getUser();

            String doAsUser = incomingRequest.getParameter(DO_AS_PARAM);
            WebResource resource =  getClient()
                    .resource(UriBuilder.fromUri(url).build().normalize())
                    .queryParam("user.name", user);
            if (doAsUser != null) {
                resource = resource.queryParam("doAs", doAsUser);
            }

            AuthenticatedURL.Token authenticationToken = null;
            if (SecurityUtil.isSecurityEnabled()) {
                UserGroupInformation ugiLoginUser = UserGroupInformation.getCurrentUser();
                LOG.debug("Security is enabled. Using DoAs : " + ugiLoginUser.getUserName());
                authenticationToken = ugiLoginUser.doAs(new PrivilegedExceptionAction<AuthenticatedURL.Token>() {
                    @Override
                    public AuthenticatedURL.Token run() throws Exception {
                        return getToken(url + PseudoAuthenticator.USER_NAME + "=" + user, getClient());
                    }
                });
            }

            ClientResponse response = resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(accept).type(mimeType)
                    .method(httpMethod, ClientResponse.class,
                            (isPost(httpMethod) ? incomingRequest.getInputStream() : null));
            incomingRequest.getInputStream().reset();

            Family status = response.getClientResponseStatus().getFamily();
            if (status == Family.INFORMATIONAL || status == Family.SUCCESSFUL) {
                return (T) response.getEntity(method.getReturnType());
            } else if (response.getClientResponseStatus().getStatusCode()
                    == Response.Status.BAD_REQUEST.getStatusCode()) {
                LOG.error("Request failed: {}", response.getClientResponseStatus().getStatusCode());
                throw FalconWebException.newAPIException(response.
                        getEntity(APIResult.class).getMessage());
            } else {
                LOG.error("Request failed: {}", response.getClientResponseStatus().getStatusCode());
                throw new FalconException(response.getEntity(String.class));
            }
        } catch (FalconWebException falconWebException) {
            LOG.error("Request failed", falconWebException);
            throw falconWebException;
        } catch (Throwable e) {
            LOG.error("Request failed", e);
            throw new FalconException(e);
        } finally {
            try {
                if (incomingRequest != null) {
                    incomingRequest.getInputStream().reset();
                }
            } catch (IOException e) {
                LOG.error("Error in HTTPChannel", e);
            }
        }
    }

    protected Client getClient() throws Exception {
        return Client.create(new DefaultClientConfig());
    }

    private boolean isPost(String httpMethod) {
        return httpMethod.equals("POST") || httpMethod.equals("PUT");
    }

    private String pathValue(Method method, Object... args) throws FalconException {

        Path pathParam = method.getAnnotation(Path.class);
        if (pathParam == null) {
            throw new FalconException("No path param mentioned for " + method);
        }
        String pathValue = pathParam.value();

        Annotation[][] paramAnnotations = method.getParameterAnnotations();
        StringBuilder queryString = new StringBuilder("?");
        for (int index = 0; index < args.length; index++) {
            if (args[index] instanceof String || args[index] instanceof Boolean || args[index] instanceof Integer
                    || args[index] instanceof List) {
                String arg = String.valueOf(args[index]);
                for (int annotation = 0; annotation < paramAnnotations[index].length; annotation++) {
                    Annotation paramAnnotation = paramAnnotations[index][annotation];
                    String annotationClass = paramAnnotation.annotationType().getName();

                    if (annotationClass.equals(QueryParam.class.getName())) {
                        if (args[index] instanceof List){
                            List lifecycle = (List) args[index];

                            if (lifecycle.size() > 0 && lifecycle.get(0) instanceof LifeCycle) {
                                List<LifeCycle> lifecycles = lifecycle;
                                for (LifeCycle l : lifecycles) {
                                    queryString.append(getAnnotationValue(paramAnnotation, "value")).append("=")
                                            .append(l.name()).append("&");
                                }
                            }
                        }else{
                            queryString.append(getAnnotationValue(paramAnnotation, "value")).
                                    append('=').append(arg).append("&");
                        }
                    } else if (annotationClass.equals(PathParam.class.getName())) {
                        pathValue = pathValue.replace("{"
                                + getAnnotationValue(paramAnnotation, "value") + "}", arg);
                    }
                }
            }
        }
        return pathValue + queryString.toString();
    }

    private String getAnnotationValue(Annotation paramAnnotation,
                                      String annotationAttribute) throws FalconException {
        try {
            return String.valueOf(paramAnnotation.annotationType().
                    getMethod(annotationAttribute).invoke(paramAnnotation));
        } catch (Exception e) {
            throw new FalconException("Unable to get attribute value for "
                    + paramAnnotation + "[" + annotationAttribute + "]");
        }
    }

    private HttpServletRequest getIncomingRequest(Object... args) {
        for (Object arg : args) {
            if (arg instanceof HttpServletRequest) {
                return (HttpServletRequest) arg;
            }
        }
        return new BufferedRequest(DEFAULT_NULL_REQUEST);
    }

    private String getHttpMethod(Method method) {
        PUT put = method.getAnnotation(PUT.class);
        if (put != null) {
            return HttpMethod.PUT;
        }

        POST post = method.getAnnotation(POST.class);
        if (post != null) {
            return HttpMethod.POST;
        }

        DELETE delete = method.getAnnotation(DELETE.class);
        if (delete != null) {
            return HttpMethod.DELETE;
        }

        return HttpMethod.GET;
    }

    private String getConsumes(Method method) {
        Consumes consumes = method.getAnnotation(Consumes.class);
        if (consumes == null || consumes.value() == null) {
            return MediaType.TEXT_PLAIN;
        }
        return consumes.value()[0];
    }

    protected AuthenticatedURL.Token getToken(String baseUrl, Client client) throws FalconException {
        AuthenticatedURL.Token currentToken = new AuthenticatedURL.Token();
        try {
            URL url = new URL(baseUrl);
            // using KerberosAuthenticator which falls back to PsuedoAuthenticator
            // instead of passing authentication type from the command line - bad factory
            HTTPSProperties httpsProperties = ((HTTPSProperties)
                    client.getProperties().get(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES));
            SSLContext sslContext = null;
            if (httpsProperties != null) {
                sslContext = httpsProperties.getSSLContext();
            }
            if (sslContext != null) {
                HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
                HttpsURLConnection.setDefaultHostnameVerifier(ALL_TRUSTING_HOSTNAME_VERIFIER);
            }
            new AuthenticatedURL(AUTHENTICATOR).openConnection(url, currentToken);
        } catch (Exception ex) {
            throw new FalconException("Could not authenticate, " + ex.getMessage(), ex);
        }

        return currentToken;
    }
}
